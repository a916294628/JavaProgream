import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//并行度的设置
//针对每一个算子设置的并行度的优先级高于全局并行度
//本程序需要两个任务的插槽
public class WordCountBingXing {
//    记得抛出异常
    public static void main(String[] args) throws Exception{
//        获取流处理的运行时的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        设置并行任务的数量为1
//        需要1个任务插槽
        env.setParallelism(1);
//        读取数据源
//        并行度设置为1
        DataStreamSource<String> stream = env.fromElements("hello world","hello world").setParallelism(1);

//        map操作
//        这里使用的flatMap方法
//        并行度设置为2
        SingleOutputStreamOperator<WordWithCount> mappedStream = stream
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        String[] arr = value.split(" ");
                        for (String e : arr) {
                            out.collect(new WordWithCount(e, 1l));
                        }

                    }
                }).setParallelism(2);
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        return value.word;
                    }
                });
        SingleOutputStreamOperator<WordWithCount> result = keyedStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });
        result.print();

        env.execute();


    }
    //    POLOS类
//    1、必须是公有类
//    2、所有字段必须是public
//    3、必须有空构造器
//    模拟了case class
    public static class WordWithCount{
        String word;
        Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
