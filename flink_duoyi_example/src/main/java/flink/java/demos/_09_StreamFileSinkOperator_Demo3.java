package flink.java.demos;
/*
 *
 * */

import flink.avro.schema.AvroEventLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 *@author AMe
 *@date 2022/8/27 17:59
 *@describe:把处理好的数据流，输出到文件系统（hdfs）
 * 使用sink算子，是拓展包中的StreamFileSink
 *
 */



public class _09_StreamFileSinkOperator_Demo3 {
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
        * 方式二：
        * 核心逻辑：
        * -利用自己的javabean类，来构造一个parquetWriterFactory
        * -利用parquetWriterFactory构造一个filesiink算子
        * -将原始数据流转成特定格式的Javabean流，输出到filesink算子
        * */

        //2、通过自己的javabean类，来得到一个parquetWriter
        ParquetWriterFactory<EventLog> parquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);




        /*

         * 构造一个StreamFileSink算子
         * */
        //3、利用生成好的parquetWriter，来构造一个支持列式输出的parquet文件的sink算子
        FileSink<EventLog> bulkSink = FileSink
                .forBulkFormat(new Path("d://dataSink/"), parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("wxf").withPartSuffix(".parquet").build())
                .build();


        //5、输出数据
        streamSource
                .sinkTo(bulkSink);

        env.execute();


    }
}
