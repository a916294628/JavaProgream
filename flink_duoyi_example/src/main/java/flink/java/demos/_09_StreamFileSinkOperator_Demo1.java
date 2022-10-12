package flink.java.demos;
/*
*
* */

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 *@author AMe
 *@date 2022/8/27 17:59
 *@describe:把处理好的数据流，输出到文件系统（hdfs）
 * 使用sink算子，是拓展包中的StreamFileSink
 *
 */



public class _09_StreamFileSinkOperator_Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt/");
        //构造一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        //将上面的流输出到文件系统

        /*
        * 通过工具，来生成一个ParquetAvroWriterFactory
        * */
        /*
        * 方式一：
        * 核心逻辑
        * -构造一个schema
        * -利用schema构造一个parquetWriterFactory
        * -利用parquetWriterFactory构造一个FileSink算子
        * -将原始数据转成GenericRecord流，输出到FileSink算子
        * */
        //1、先定义GenericRecord的数据模式
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord")
                .namespace("flink.avro.schema")
                .doc("用户行为时间数据模式")
                .fields()
                    .requiredLong("guid")
                    .requiredString("sessionId")
                    .requiredString("eventId")
                    .requiredLong("timeStamp")

                    //以下三行是一体的，eventInfo的类型是map
                    .name("eventInfo")
                    .type()
                    .map()

                    //map里的值的类型是string
                    .values()
                    .type("string")
                    .noDefault()
                //还是map的builder，可以默认值，没默认值
                .endRecord();
        //2、通过定义好的Schema模式，来得到一个parquetWriter
        //GenericRecord里没有类型，所以是需要schema里返回类型
        ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);


        /*

        * 构造一个StreamFileSink算子
        * */
        //3、利用生成好的parquetWriter，来构造一个支持列式输出的parquet文件的sink算子
        FileSink<GenericRecord> sink1 = FileSink
                .forBulkFormat(new Path("d://dataSink/"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("wxf").withPartSuffix(".parquet").build())
                .build();

        //4、将自定义的javabean的流转成上述sink算子中parquetWriter所需的 GenericRecord流
        SingleOutputStreamOperator<GenericRecord> recordStream = streamSource
                .map((MapFunction<EventLog, GenericRecord>) eventLog -> {
                    //构造一个Record对象
                    GenericData.Record record = new GenericData.Record(schema);
                    //将数据填入record
                    record.put("guid", eventLog.getGuid());
                    record.put("sessionId", eventLog.getSessionId());
                    record.put("eventId", eventLog.getEventId());
                    record.put("timeStamp", eventLog.getTimeStamp());
                    record.put("eventInfo", eventLog.getEventInfo());
                    return record;
                }).returns(new GenericRecordAvroTypeInfo(schema));//设置返回的泛型，进行序列化，因为要将map到分布式task中去执行，必须进行序列化，returns在Lambda表达式中才能生效
        //由于avro的 相关类、对象需要用avro的序列化器，所以需要显式指定AvroTypeInfo来提供AvroSerializable
        /*
        * 不同的序列化机制，对同一个对象序列化后产生的字节，是不同因为他们的序列化规则和编码规则不同
        *
         * */



        //5、输出数据
        recordStream
                .sinkTo(sink1);

        env.execute();


    }
}
