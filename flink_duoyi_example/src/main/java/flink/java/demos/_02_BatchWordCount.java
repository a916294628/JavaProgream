package flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class _02_BatchWordCount {
    public static void main(String[] args) throws Exception{
        //批计算入口环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        //读数据：批计算中得到的数据抽象是一个DataSet
        DataSource<String> stringDataSource = batchEnv.readTextFile("G:\\JavaProgram\\flink_example\\data\\wc\\input\\wc.txt");

        //在dataset上调用各种dataset算子
        stringDataSource
                .flatMap(new MyFlatMapFunction())
                //批处理是groupby
                .groupBy(0)
                .sum(1)
                .print();

    }
}

class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split("\\s+");
        for (String word : words) {
            collector.collect(Tuple2.of(word,1));
        }
    }
}