package flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class _18_ChannalSelector_Partition_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.137.131",9999);

        DataStream<String> s2 = s1.map(tp -> tp.toUpperCase())
                .setParallelism(4)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] arr = s.split(",");
                        for (String a : arr) {
                            collector.collect(a);
                        }
                    }
                })
                .setParallelism(4)
                .forward();
        SingleOutputStreamOperator<String> s3 = s2.map(s -> s.toLowerCase()).setParallelism(4);

        SingleOutputStreamOperator<String> s4 = s3.keyBy(s->s.substring(0,2))
                .process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(s + ">");
            }
        }).setParallelism(4);

        SingleOutputStreamOperator<String> s5 = s4.filter(s -> s.startsWith("b")).setParallelism(4);
        s5.print().setParallelism(4);

        env.execute();


    }
}
