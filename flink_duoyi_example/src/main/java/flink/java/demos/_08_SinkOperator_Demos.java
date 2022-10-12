package flink.java.demos;

import com.alibaba.fastjson.JSON;
import flink.avro.schema.AvroEventLog;
import lombok.*;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;

public class _08_SinkOperator_Demos {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());


        //把它打印：输出到控制台
//        streamSource.print();

        //输出到文件
//        streamSource.writeAsText("d:/sink_test", FileSystem.WriteMode.OVERWRITE);
//        streamSource.map(bean -> Tuple5.of(bean.getEventId(),bean.getEventInfo(),bean.getGuid(),bean.getSessionId(),bean.getTimeStamp())).returns(new TypeHint<Tuple5<String, Map<String, String>, Long, String, Long>>() {
//        }).writeAsCsv("d:/sink_test", FileSystem.WriteMode.OVERWRITE);

        //应用StreamFileSink 算子，来将数据输出到文件系统
        //构造一个FileSink对象
        FileSink<String> rowSink = FileSink
                .forRowFormat(new Path("d:/filesink/"), new SimpleStringEncoder<String>("utf-8"))
                /*文件的滚动策略（间隔时间10s，或文件达到1m，就进行切分）*/
                .withRollingPolicy(DefaultRollingPolicy
                        .builder()
                        .withRolloverInterval(10000)
                        .withMaxPartSize(1024 * 1024)
                        .build())
                /*分桶策略（划分子文件夹的策略）*/
                .withBucketAssigner(new DateTimeBucketAssigner<String>())
                /*5毫秒检查一次是否分桶*/
                .withBucketCheckInterval(5)
                /*输出文件文件名的配置。，前缀，后缀*/
                .withOutputFileConfig(OutputFileConfig
                        .builder()
                        .withPartPrefix("wxf")
                        .withPartSuffix(".txt")
                        .build())
                .build();
        //然后添加到流进行输出
        //bean类直接输出不好输出，可以将该流转换为json
        streamSource.map(JSON::toJSONString)
//                .addSink() /*SinkFunction实现类对象，AddSink（）来添加*/
                        .sinkTo(rowSink);/*sink 的实现类对象，用sinkTo来添加*/

        /*
        * 2.输出为列格式
        * parquest文件中带有schema
        * schema(表结构)：字段名，字段数据类型，字段顺序，字段的嵌套结构
        * 要生成parquet需要先定义schema而定义schema有多种方式
        *
        * 要构造一个列模式的FileSink，需要一个ParquetAvroWriterFactory
        * 而获得ParaquetAvroWriterFactory的方式是，利用一个工具类:ParquetAvroWriters
        * 这个工具类提供了3中方法，来为用户构造一个ParquetAvroWriterFactory
        *
        * 方法1：
        * ParquetAvroWriters.forGenericRecord()
        * 方法2：
        * writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLog.class)
        * 方法3：
        *writerFactory2 = ParquetAvroWriters.forReflectRecord(EventLog.class)
        * */
        //方式1：手动构造schema，来生成ParaquetAvroWriter工厂
//        Schema schema = Schema.createRecord("id", "用户id", "cn.doitedu.User", true);
//        ParquetAvroWriters.forGenericRecord() //根据通用的方法及传入的信息，来获取avra模式的schema，并生成对应的ParaquetWriter

        //方式2：利用Avro的规范Bean对象，来生成ParaquetAvroWriter工厂
        ParquetWriterFactory<AvroEventLog> writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLog.class);//需要写avsc文件，并根据文件生成javaBean，然后该方法可以从这种javaBean来自动反射获取schema

        FileSink<AvroEventLog> parquestSink = FileSink
                .forBulkFormat(new Path("d:/bulksink/"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withBucketCheckInterval(5)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())//bulk模式下的文件滚动策略 只有一种：当checkpoint发生下，进行文件滚动
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("wxf").build())
                .build();

        //将上面的构造好的sink算子对象，添加到数据流，进行数据转换
        streamSource
//                下面语句会报错，是一个java类的继承的一个问题，对于evenyLogBean.getEventInfo()返回的是map（string，string），而AvroEventLog.EventInfo()构造需要map（CharSequence，CharSequence）
//                虽然String是CharSequence的子类，但是map（string，string）与，map(CharSequence，CharSequence)都属于map，是一个级别的，
//                因此可以将map（string，string）通过hashmap进行转换
//                .map(evenyLogBean->new AvroEventLog(evenyLogBean.getGuid(),evenyLogBean.getSessionId(),evenyLogBean.getEventId(),evenyLogBean.getTimeStamp(),evenyLogBean.getEventInfo()))
                .map(eventLogBean->{
                    HashMap<CharSequence, CharSequence> eventInfo = new HashMap<>();
                    for (Map.Entry<String, String> entry : eventLogBean.getEventInfo().entrySet()) {
                        eventInfo.put(entry.getKey(),entry.getValue());
                    }
                    return new AvroEventLog(eventLogBean.getGuid(),eventLogBean.getSessionId(),eventLogBean.getEventId(),eventLogBean.getTimeStamp(),eventInfo);
                }).returns(AvroEventLog.class)
                .sinkTo(parquestSink);
        //方式3：利用Avro的规范Bean对象，来生成ParaquetAvroWriter工厂
        ParquetWriterFactory<EventLog> writerFactory2 = ParquetAvroWriters.forReflectRecord(EventLog.class);//该方法，传入一个普通的JavaBean类，就可以自动通过反射来生成Schema

        FileSink<EventLog> parquetSink2 = FileSink
                .forBulkFormat(new Path("d:/bulksink/"), writerFactory2)
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withBucketCheckInterval(5)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())//bulk模式下的文件滚动策略 只有一种：当checkpoint发生下，进行文件滚动
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("wxf").build())
                .build();
        streamSource.sinkTo(parquetSink2);


        env.execute();

    }


}


