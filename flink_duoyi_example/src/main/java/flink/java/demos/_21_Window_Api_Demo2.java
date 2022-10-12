package flink.java.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class _21_Window_Api_Demo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = source.map(tp -> {
            String[] arr = tp.split(",");
            return new EventBean2(Long.parseLong(arr[0]), arr[1], Long.parseLong(arr[2]), arr[3],Integer.parseInt(arr[4]));
        }).returns(EventBean2.class);

        /*
        * 全局窗口开窗api
        * */

        //全局 计数滚动窗口
        beanStream.countWindowAll(10)
                        .apply(new AllWindowFunction<EventBean2, String, GlobalWindow>() {
                            @Override
                            public void apply(GlobalWindow globalWindow, Iterable<EventBean2> iterable, Collector<String> collector) throws Exception {

                            }
                        });
        //全局 计数滑动窗口
        beanStream.countWindowAll(10,2);// 窗口长度为10条数据，滑动步长为2条数据

        //全局 事件时间滚动窗口
        beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))//窗口长度为30s的滚动窗口
                .apply(new AllWindowFunction<EventBean2, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<EventBean2> iterable, Collector<String> collector) throws Exception {

                    }
                });
        //全局 事件时间滑动窗口
        beanStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)));//窗口大小为30s，滑动步长10s
        //全局 事件时间会话窗口
        beanStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30)));//前后两个事件时间的时间超过30s，就划分窗口

        //全局 处理时间滚动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));

        //全局 处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)));

        //全局 处理时间会话窗口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

        /*
        * 二：各种keyed窗口开窗api
        *
        * */

        KeyedStream<EventBean2, Long> keyedStream = beanStream.keyBy(EventBean2::getGuid);

        // Keyed 计数滚动窗口
        keyedStream.countWindow(10);


        // Keyed 计数滑动窗口
        keyedStream.countWindow(10, 2);


        // Keyed 事件时间滚动窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(30)));


        // Keyed 事件时间滑动窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));


        // Keyed  事件时间会话窗口
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)));


        // Keyed  处理时间滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));


        // Keyed  处理时间滑动窗口
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));


        // Keyed  处理时间会话窗口
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));



        env.execute();
    }
}
