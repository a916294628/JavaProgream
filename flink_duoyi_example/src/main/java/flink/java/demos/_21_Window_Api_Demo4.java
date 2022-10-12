package flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

public class _21_Window_Api_Demo4 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> source = env.socketTextStream("192.168.137.131", 9999);

        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> beanStream = source.map(s -> {
            String[] split = s.split(",");
            EventBean2 bean = new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
            return Tuple2.of(bean, 1);
        }).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<EventBean2,Integer>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean2, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<EventBean2, Integer> element, long l) {
                                return element.f0.getTimeStamp();
                            }
                        }));

        OutputTag<Tuple2<EventBean2, Integer>> lateDataOutputTag = new OutputTag<>("late_data", TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {}));

        SingleOutputStreamOperator<String> sulterStream = beanStream.keyBy(tp -> tp.f0.getGuid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //设置自定义的Trigger
                .trigger(MyEventTimeTrigger.create())
                //设置自定义的Evictor ，它会在窗口触发计算之前，对窗口中的e0x标记事件进行移除

                .evictor(MyTimeEvictor.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<EventBean2, Integer>, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<Tuple2<EventBean2, Integer>> iterable, Collector<String> collector) throws Exception {
                        int count = 0;
                        for (Tuple2<EventBean2, Integer> eventBean2IntegerTuple2 : iterable) {
                            count++;

                        }
                        collector.collect(timeWindow.getStart() + ":" + timeWindow.getEnd() + "," + count);
                    }
                });

        DataStream<Tuple2<EventBean2, Integer>> lateDataSideStream = sulterStream.getSideOutput(lateDataOutputTag);

        sulterStream.print("主流结果");

        lateDataSideStream.print("迟到数据");

        env.execute();


    }
}

class MyEventTimeTrigger extends Trigger<Tuple2<EventBean2,Integer>,TimeWindow>{
    private MyEventTimeTrigger() {}

    /*
    * 来一条数据时，需要检查watermark是否已经越过窗口结束点需要触发
    * */

    @Override
    public TriggerResult onElement(
            Tuple2<EventBean2, Integer> element, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        //如果窗口的结束点 <= 当前的watermark
        if (timeWindow.maxTimestamp() <= triggerContext.getCurrentWatermark()){
            return TriggerResult.FIRE;
        } else {
            //注册定时器，定时器的触发事件为：窗口的结束时间

            triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp());

            //判断，当前的数据的用户行为事件id是否等于e0x，如是，则触发
            if ("e0x".equals(element.f0.getEvrntId())) return TriggerResult.FIRE;

            return TriggerResult.CONTINUE;
        }


    }

    /*
    * 当处理时间定时器的触发时间（窗口的结束点时间）到达了，检查是否满足触发条件
    *
    * */

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }
    /*
    *
    * 当事件时间的定时器的触发事件（窗口的结束点时间）到达了，检查是否满足触发条件
     * 下面的方法是定时器在调用
    * */

    @Override
    public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return time == timeWindow.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());
    }
    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    public static MyEventTimeTrigger create() { return new MyEventTimeTrigger();}

}

class MyTimeEvictor implements Evictor<Object,TimeWindow>{
    private static final long serialVersionUID = 1L;

    private final long windowSize;
    private final boolean doEvictAfter;

    public MyTimeEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyTimeEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    //窗口出发前，调用
    @Override
    public void evictBefore(
            Iterable<TimestampedValue<Object>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
        if(!doEvictAfter){

        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

    }

    /**
     * 元素移除的核心逻辑
     */
    public void evict(Iterable<TimestampedValue<Object>> elements,int size, EvictorContext ctx){
        if(!hasTimestamp(elements)){
            return;
        }
        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
            iterator.hasNext();){
            TimestampedValue<Object> record = iterator.next();

            Tuple2<EventBean2,Integer> tuple = (Tuple2<EventBean2, Integer>) record.getValue();

            // 加了一个条件： 数据的eventId=e0x，也移除
            if (record.getTimestamp() <= evictCutoff  || tuple.f0.getEvrntId().equals("e0x")) {
                iterator.remove();
            }
        }

    }

    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
        Iterator<TimestampedValue<Object>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /**
     * 用于计算移除的时间截止点
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }
    public static   MyTimeEvictor  of(Time windowSize) {
        return new MyTimeEvictor(windowSize.toMilliseconds());
    }

}
