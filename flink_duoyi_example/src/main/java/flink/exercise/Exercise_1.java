package flink.exercise;


import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
* 创建两个流
* 流1：
* “id,eventId,cnt”
* 1,event01,3
* 2,event02,2
* 3,event03,4
* 流2：
* “id,gender,city”
* 1,male,shanghai
* 2,female,beijing
* 需求：
* 1、把数据流一的数据展开
* 比如，将一条数据：1，event01,3
* 需要展开成三条数据：
* 1，event01,随机数1
* 2,event01,随机数2
* 3,event01,随机数3
*
* 2、流1的数据，还需要关联流2的数据（性别，城市）
* 3、并且把数据关联失败的流1的数据，写入到一个测流；否则输出到主流
* 4、把对主流数据按性别分组，取最大随机数所在的呢一条的数据作为输出结果
* 5、把对测流处理结果写入文件系统，并写成parquet格式
* 6、把主流处理结果写入到mysql，并实现幂等更新
*
*
* */
public class Exercise_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        DataStreamSource<String> socket1 = env.socketTextStream("192.168.137.131", 9991);
        SingleOutputStreamOperator<EventInfo> s1 = socket1.map(tp -> {
            String[] arr = tp.split(",");
            return new EventInfo(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        });
        s1.print("s1");

        DataStreamSource<String> socket2 = env.socketTextStream("192.168.137.131", 9992);
        SingleOutputStreamOperator<UserInfo> s2 = socket2.map(tp -> {
            String[] arr = tp.split(",");
            return new UserInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });
        s2.print("s2");
        SingleOutputStreamOperator<EventInfo> processedStream = s1.process(new ProcessFunction<EventInfo, EventInfo>() {
            @Override
            public void processElement(EventInfo eventInfo, ProcessFunction<EventInfo, EventInfo>.Context context, Collector<EventInfo> collector) throws Exception {
                int num = eventInfo.getCut();
                for (int i = 0; i < num; i++) {
                    collector.collect(new EventInfo(eventInfo.getId(), eventInfo.getEventId(), RandomUtils.nextInt(10, 100)));
                }
            }
        });

        processedStream.print("processedStream");

        MapStateDescriptor<Integer, UserInfo> userInfoStateDesc = new MapStateDescriptor<>("userInfo", Integer.class, UserInfo.class);

        BroadcastStream<UserInfo> broadcastStream = s2.broadcast(userInfoStateDesc);
        BroadcastConnectedStream<EventInfo, UserInfo> connected = processedStream.connect(broadcastStream);
        SingleOutputStreamOperator<EventUser> joinedStream = connected.process(new BroadcastProcessFunction<EventInfo, UserInfo, EventUser>() {
            @Override
            public void processElement(EventInfo eventInfo, BroadcastProcessFunction<EventInfo, UserInfo, EventUser>.ReadOnlyContext readOnlyContext, Collector<EventUser> collector) throws Exception {
                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = readOnlyContext.getBroadcastState(userInfoStateDesc);
                UserInfo userinfo  = null;
                System.out.println(userinfo);
                if (broadcastState != null && (userinfo = broadcastState.get(eventInfo.getId()))!= null) {
                    collector.collect(new EventUser(eventInfo.getId(), eventInfo.getEventId(), eventInfo.getCut(), userinfo.getGender(), userinfo.getCity()));
                } else {
                    readOnlyContext.output(new OutputTag<>("none", TypeInformation.of(EventInfo.class)), eventInfo);
                }
            }

            @Override
            public void processBroadcastElement(UserInfo userInfo, BroadcastProcessFunction<EventInfo, UserInfo, EventUser>.Context context, Collector<EventUser> collector) throws Exception {
                BroadcastState<Integer, UserInfo> broadcastState = context.getBroadcastState(userInfoStateDesc);
                broadcastState.put(userInfo.getId(), userInfo);
            }
        });

        joinedStream.print("joinedStream");

        SingleOutputStreamOperator<EventUser> maxedStream = joinedStream.keyBy(EventUser::getGender).maxBy("num");

        maxedStream.print("maxedStream ");

        SinkFunction<EventUser> jdbcSink = JdbcSink.sink(
                "insert into t_eventuser values(?,?,?,?,?) on duplicate key update id = ?,eventid = ?,cut = ?,city = ?",
                new JdbcStatementBuilder<EventUser>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventUser eventUser) throws SQLException {
                        preparedStatement.setInt(1, eventUser.getId());
                        preparedStatement.setString(2, eventUser.getEventId());
                        preparedStatement.setInt(3, eventUser.getNum());
                        preparedStatement.setString(4, eventUser.getGender());
                        preparedStatement.setString(5, eventUser.getCity());

                        preparedStatement.setInt(6, eventUser.getId());
                        preparedStatement.setString(7, eventUser.getEventId());
                        preparedStatement.setInt(8, eventUser.getNum());
                        preparedStatement.setString(9, eventUser.getCity());


                    }
                },
                JdbcExecutionOptions.builder().withMaxRetries(3).withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );
        maxedStream.addSink(jdbcSink);
        DataStream<EventInfo> noneStream = joinedStream.getSideOutput(new OutputTag<>("none", TypeInformation.of(EventInfo.class)));
        noneStream.print("noneStream");
        ParquetWriterFactory<EventInfo> parquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventInfo.class);

        FileSink<EventInfo> bulkSink = FileSink.forBulkFormat(new Path("d:/sidesink/"), parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventInfo>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("wxf_").build())
                .build();
        noneStream.sinkTo(bulkSink);

        env.execute();


    }
}
