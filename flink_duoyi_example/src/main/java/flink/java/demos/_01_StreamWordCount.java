package flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ConfigurationException;

public class _01_StreamWordCount {
    /**
     * 通过socket数据源，去请求一个socket服务（nc -lk 9999）
     * 然后统计数据流程中出现的单词及个数
     * */
    public static void main(String[] args) throws Exception{

        /**
         * 本地运行模式时，程序的默认并行度为，cpu的逻辑核数
         *
         * */
        //创建一个编程入口环境
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();//之前的批处理运行环境，现在已经流批统一
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//流批一体的入口环境

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        env.setParallelism(1);
        //通过source算子，映射数据源为一个datastream（数据流）
        DataStreamSource<String> source = env.socketTextStream("192.168.137.131", 9998);


        //然后通过算子对数据流进行各种转换（计算逻辑）
        SingleOutputStreamOperator<Tuple2<String, Integer>> word1 = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        //切单词
                        String[] arr = s.split(" ");
                        for (String word : arr) {
                            //返回每一对（单词，1）
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                });
        KeyedStream<Tuple2<String, Integer>, String> keyedstream = word1
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedstream.sum("f1");




        //通过sink算子，然后将讲过输出
        result.print();

        //触发程序的提交运行
        env.execute();
    }
}
