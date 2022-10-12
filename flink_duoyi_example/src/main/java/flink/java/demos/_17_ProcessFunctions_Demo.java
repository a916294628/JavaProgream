package flink.java.demos;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;


public class _17_ProcessFunctions_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.prot",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        //id,eventId
        DataStreamSource<String> stream1 = env.socketTextStream("hadoop", 9998);

        /*
        * 在普通的流datastream上调用process算子，传入的是“ProcessFunction”
        * */
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            //可以使用 生命周期open方法

            @Override
            public void open(Configuration parameters) throws Exception {
                //可以调用 getRuntimeContext 方法拿到各种运行时的上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();

                super.open(parameters);
            }

            @Override
            public void processElement(String s, ProcessFunction<String, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                //可以做测流输出
                context.output(new OutputTag<>("s1", Types.STRING), s);

                //可以做主流输出
                String[] arr = s.split(",");
                collector.collect(Tuple2.of(arr[0], arr[1]));


            }
            //可以使用 生命周期close方法

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
        /*
        * 在 keyedStream上调用process算子 传入的是“KeyedProcessFunction”
        * KeyedProcessFunction 中的 ，泛型1 ：流中的key的类型 泛型2: 流中的数据的类型  ；泛型三：处理后的输出的结果的类型
        * */
        //对s1流进行keyby分组
        KeyedStream<Tuple2<String, String>, String> keyedStream = s1.keyBy(tp -> tp.f0);
        //然后在keyby后的流上调用process算子
        SingleOutputStreamOperator<Tuple2<Integer, String>> s2 = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> stringStringTuple2, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context context, Collector<Tuple2<Integer, String>> collector) throws Exception {
                collector.collect(Tuple2.of(Integer.parseInt(stringStringTuple2.f0), stringStringTuple2.f1.toUpperCase()));
            }
        });

        env.execute();

    }
}
