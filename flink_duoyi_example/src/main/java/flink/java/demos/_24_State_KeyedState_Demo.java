package flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _24_State_KeyedState_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        // 开启快照后，就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");

        //开启 task级别故障自动failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 需要使用map算子来达到一个效果：
        // 没来一条数据（字符串），输出 该条字符串拼接此前到达过的所有字符串
        source
                .keyBy(s->"0")
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        RuntimeContext runtimeContext = getRuntimeContext();
                        //获取一个list结构的状态存储器
                        listState = runtimeContext.getListState(new ListStateDescriptor<String>("lst", String.class));

                        //获取一个  单值 结构的状态存储器
                        //ValueState

                        //获取一个Map结构的状态存储器
                        MapState<String, String> mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("xx", String.class, String.class));


                    }

                    @Override
                    public String map(String s) throws Exception {

                        //将本条数据，装入状态存储器

                        listState.add(s);

                        //遍历所有的历史字符串，拼接结果
                        StringBuilder sb = new StringBuilder();

                        for (String s1 : listState.get()) {
                            sb.append(s);
                        }
                        return sb.toString();
                    }
                }).setParallelism(2)
                .print().setParallelism(2);
        // 提交一个job
        env.execute();

    }
}
