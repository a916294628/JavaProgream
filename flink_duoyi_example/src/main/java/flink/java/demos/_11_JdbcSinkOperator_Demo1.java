package flink.java.demos;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
public class _11_JdbcSinkOperator_Demo1 {
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

        //构造一个jdbcsink算子
        /*
        * (不保证exactly-once的一种)
        * */
        SinkFunction<EventLog> jdbcSink = JdbcSink
                .sink(
                        "insert into t_eventlog values(?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                        new JdbcStatementBuilder<EventLog>() {
                            @Override
                            public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                                stmt.setLong(1, eventLog.getGuid());
                                stmt.setString(2, eventLog.getSessionId());
                                stmt.setString(3, eventLog.getEventId());
                                stmt.setLong(4, eventLog.getTimeStamp());
                                stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));


                                stmt.setString(6, eventLog.getSessionId());
                                stmt.setString(7, eventLog.getEventId());
                                stmt.setLong(8, eventLog.getTimeStamp());
                                stmt.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                            }
                        },
                        JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUsername("root")
                                .withPassword("root")
                                .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=UTC")
                                .build()
                );
//        streamSource.addSink(jdbcSink);

        //构造一个jdbcsink算子
        /*
         * (保证exactly-once的一种)
         * 底层是利用jdbc目标数据库的事务机制的
         * */

        SinkFunction<EventLog> eosJdbcSink = JdbcSink
                .exactlyOnceSink(
                        "insert into t_eventlog values(?,?,?,?,?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                        new JdbcStatementBuilder<EventLog>() {
                            @Override
                            public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                                stmt.setLong(1, eventLog.getGuid());
                                stmt.setString(2, eventLog.getSessionId());
                                stmt.setString(3, eventLog.getEventId());
                                stmt.setLong(4, eventLog.getTimeStamp());
                                stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                                stmt.setString(6, eventLog.getSessionId());
                                stmt.setString(7, eventLog.getEventId());
                                stmt.setLong(8, eventLog.getTimeStamp());
                                stmt.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                            }
                        },
                        JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                        JdbcExactlyOnceOptions.builder()
                                //mysql不支持同一个连接上存在并行的多个事务，必须吧该参数设置为true
                                .withTransactionPerConnection(true).build(),
                        new SerializableSupplier<XADataSource>() {
                            @Override
                            public XADataSource get() {
                                MysqlXADataSource xADataSource = new MysqlXADataSource();
                                xADataSource.setUrl("jdbc:mysql://localhost:3306/test?serverTimezone=UTC");
                                xADataSource.setUser("root");
                                xADataSource.setPassword("root");
                                return xADataSource;
                            }
                        }

                );
        streamSource.addSink(eosJdbcSink);



        env.execute();


    }
}
