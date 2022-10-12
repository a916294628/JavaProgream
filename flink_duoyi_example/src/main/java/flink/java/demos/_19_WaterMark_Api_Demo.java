package flink.java.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class _19_WaterMark_Api_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        // 1,e01,168673487846,pg01
        SingleOutputStreamOperator<String> s1 = env.socketTextStream("hadoop01", 9999).disableChaining();

        // 策略1： WatermarkStrategy.noWatermarks()  不生成 watermark，禁用了事件时间的推进机制
        // 策略2： WatermarkStrategy.forMonotonousTimestamps()  紧跟最大事件时间
        // 策略3： WatermarkStrategy.forBoundedOutOfOrderness()  允许乱序的 watermark生成策略
        // 策略4： WatermarkStrategy.forGenerator()  自定义watermark生成算法

        /*
        * 实例 一：从源头算子开始生成watermark
        * */
        // 1、构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0)) //允许乱序的算法策略
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        return Long.parseLong(s.split(",")[2]);
                    }
                });
        // 2、将构造好的 watermark策略对象，分配给流（source算子）
//        SingleOutputStreamOperator<String> watermarkStream = s1.assignTimestampsAndWatermarks(watermarkStrategy);



        /*
         * 示例 二：  不从最源头算子开始生成watermark，而是从中间环节的某个算子开始生成watermark
         * 注意！：如果在源头就已经生成了watermark， 就不要在下游再次产生watermark
         */
        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] arr = s.split(",");
                    return new EventBean(Long.parseLong(arr[0]), arr[1], Long.parseLong(arr[2]), arr[3]);
                }).returns(EventBean.class).disableChaining()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                                    @Override
                                    public long extractTimestamp(EventBean eventBean, long l) {
                                        return eventBean.getTimeStamp();
                                    }
                                })
                );

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {
                //打印此刻的watermark
                long processTime = context.timerService().currentProcessingTime();
                long watermark = context.timerService().currentWatermark();
                System.out.println("本次收到的数据"+eventBean);
                System.out.println("此刻的watermark"+watermark);
                System.out.println("此刻的处理时间（ProcessingTime）"+processTime);
                collector.collect(eventBean);
            }
        }).print();

        env.execute();

    }
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class EventBean{
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
}
