package flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *@author AMe
 *@date 2022/8/28 21:31
 *@describe:
 *测输出流，代码演示（process算子）
 */
public class _13_SideOutput_Demo {
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

        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             * @Description:蒋兴伟事件流，进行分流
             * appLaunch事件放入一个流
             * putBack事件放入一个流
             * 其他事件保留到主流
             *
             * @param: eventLog
             * @param: context
             * @param: collector
             * @return: void
             * @Author: hello
             * @Date 2022/8/29 22:07
             */
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context context, Collector<EventLog> collector) throws Exception {
                String eventId = eventLog.getEventId();
                if ("appLaunch".equals(eventId)) {
                    context.output(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)), eventLog);
                } else if ("putBack".equals(eventId)) {
                    context.output(new OutputTag<>("back", TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                }

                collector.collect(eventLog);

            }
        });
        //获取launch测数据流
        DataStream<EventLog> launchStream = processed.getSideOutput(new OutputTag<>("launch", TypeInformation.of(EventLog.class)));
//      获取back测数据流
        DataStream<String> backStream = processed.getSideOutput(new OutputTag<>("back", TypeInformation.of(String.class)));

        launchStream.print("launch");
        backStream.print("back");


        env.execute();


    }
}
