package flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception{

        //流处理的编程入口
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        streamEnv.setParallelism(1);
        //设置运行模式，对于批模式可以设置成批模式，会最后输出结果
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);//安批计算模式
//        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);//按流模式执行
//        streamEnv.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//flink自己判断
        //读文件，得到dataStream
        DataStreamSource<String> streamSource = streamEnv.readTextFile("G:\\JavaProgram\\flink_duoyi_example\\data\\wc\\input\\wc.txt");

        //调用dataStream的算子做计算
        streamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = s.split("\\s+");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word,1));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();
        streamEnv.execute();

    }
}
