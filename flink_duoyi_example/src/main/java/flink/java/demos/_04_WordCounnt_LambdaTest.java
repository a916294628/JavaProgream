package flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class _04_WordCounnt_LambdaTest{
    public static void main(String[] args) throws Exception{


        //创建一个入口环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();

        //流式处理入口环境
        DataStreamSource<String> streamSource = envStream.readTextFile("G:\\JavaProgram\\flink_duoyi_example\\data\\wc\\input\\wc.txt");
        //先把句子变大写
        /**从map算子接收的MapFunction接口实现来看，他是一个单抽象的方法接口
         * 所以这个接口的实现类的核心功能，就在它的方法上
         * 那就可以用lambda表达式来简洁实现
        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return null;
            }
        });
         */

        //lambda表达式怎么写，看你要实现的那个接口要接受什么参数，返回什么结果
        //然后就按lambda语法来表达：(参数1，参数2，.....) ->{函数体}

        //streamSource.map((value) -> {return value.toUpperCase();});

        //由于上面的lambda表达式，参数列表只有一个，其函数体只有一行代码，则可以简化，()可以取消，{}可以取消,return可以取消
        //streamSource.map(value -> value.toUpperCase());

        //由于上面的lambda表达式，函数体只有一行代码，且其中的方法调用没有参数传递，则可以将方法调用，转成“方法引用”,由于不需要将参数传给谁
        //因此value也可以不用要
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);


        //然后切成单次，并变成（单词，1），并压平
        /*upperCased.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            }
        });*/

        //方法接回的参数多不太建议写成lambda表达式，但是也可以写
        //怎么知道接收哪些参数呢？可以从它的接口看。
        //从上面的接口看，他依然是一个单抽象方法的接口，所以它的方法实现，依然可以用lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperCased.flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = s.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }

        }).returns(Types.TUPLE(TypeInformation.of(String.class),Types.INT));
                //.returns(Types.TUPLE(Types.STRING,Types.INT)); //利用工具类Types的各种静态方法，来生成TypeInformation
                //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));//更通用的，是传入TypeInformation，下面的TypeHint也是封装了TypeInformation
                //.returns(new TypeHint<Tuple2<String, Integer>>() {});  //通过TypeHint传返回数据类型

        //按单词分组
        /*wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return null;
            }
        });*/
        //从上面的KeySelector接口看，他依然是一个单抽象方法的接口，所以它的方法实现，依然可以用lambda表达式

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(value -> value.f0);

        //统计单词个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        result.print();

        //输出结果

        envStream.execute();
    }



}
