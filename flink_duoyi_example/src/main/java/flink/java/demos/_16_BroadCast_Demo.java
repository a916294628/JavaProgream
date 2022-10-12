package flink.java.demos;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class _16_BroadCast_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });


        /**
         * 案例背景：
         *    流 1：  用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
         *    流 2：  用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不定 （作为广播流）
         *
         *    需要加工流1，把用户的维度信息填充好，利用广播流来实现
         */

        // 将字典数据所在流： s2  ，  转成 广播流
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<>("userInfoStateDesc", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastStream<Tuple3<String, String, String>> s2BroadCastStream = s2.broadcast(userInfoStateDesc);

        //哪个流处理需要广播状态数据，就要去连接connect 这个广播流

        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected = s1.connect(s2BroadCastStream);

        /*
        * 对连接广播流之后的“连接流”进行处理
        * 核心思想：
        * 在processBroadcastElement方法中，把获得到的广播流中的数据，插入到“广播状态中”
        * 在processElement方法中，对取得的主流数据进行处理（从广播状态中获取想要拼接的数据，拼接后输出）
        *
        *
        * */

        SingleOutputStreamOperator<String> resultStream = connected.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
            /*BroadcastState<String, Tuple2<String, String>> broadcastState;*/

            /**
             * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
             * @param stringStringTuple2  左流（主流）中的一条数据
             * @param readOnlyContext  上下文
             * @param collector  输出器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> stringStringTuple2, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                //通过ReadOnlyContext 取得广播对象，是一个“只读”的对象
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = readOnlyContext.getBroadcastState(userInfoStateDesc);

                if (broadcastState != null) {
                    Tuple2<String, String> userInfo = broadcastState.get(stringStringTuple2.f0);
                    collector.collect(stringStringTuple2.f0 + "," + stringStringTuple2.f1 + "," + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));

                } else {
                    collector.collect(stringStringTuple2.f0 + "," + stringStringTuple2.f1 + "," + null + "," + null);
                }
            }

            /**
             * @Description:
             *
             * @param: stringStringStringTuple3 广播流中的一条数据
             * @param: context 上下文
             * @param: collector 输出器
             * @return: void
             * @Author: hello
             * @Date 2022/8/30 22:51
             */

            @Override
            public void processBroadcastElement(Tuple3<String, String, String> stringStringStringTuple3, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context context, Collector<String> collector) throws Exception {
                //从上下文中获取广播状态对象（可读可写的状态对象）
                BroadcastState<String, Tuple2<String, String>> broadcastState = context.getBroadcastState(userInfoStateDesc);

                //然后将获得的这条广播数据流，拆分后，装入广播状态
                broadcastState.put(stringStringStringTuple3.f0, Tuple2.of(stringStringStringTuple3.f1, stringStringStringTuple3.f2));

            }
        });
        resultStream.print();
        env.execute();
    }
}
