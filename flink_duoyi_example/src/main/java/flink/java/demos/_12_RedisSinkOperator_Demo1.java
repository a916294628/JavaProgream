package flink.java.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *@author AMe
 *@date 2022/8/28 21:31
 *@describe:
 *将数据流写入mysql，利用jdbcsink算子
 * 测试准备：在mysql中建表
 * CREATE TABLE t_eventlog(
 * guid BIGINT(20) NOT NULL PRIMARY KEY,
 * sessionId VARCHAR(255) DEFAULT NULL,
 * eventId VARCHAR(255) DEFAULT NULL,
 * ts BIGINT(20) DEFAULT NULL,
 * eventInfo VARCHAR(255) DEFAULT null
 * );
 */
public class _12_RedisSinkOperator_Demo1 {
    public static void main(String[] args) throws Exception {
        //开启一个本地Flinkweb
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt/");
        //构造一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());





        env.execute();


    }
}
