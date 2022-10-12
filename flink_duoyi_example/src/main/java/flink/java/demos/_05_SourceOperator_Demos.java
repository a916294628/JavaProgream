package flink.java.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception{
        //一到多个可变元素
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);//默认并行度为1
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5, 6);

        fromElements.map(s->s*10)/*.print()*/;

        //将元素作为一个集合
        List<String> dataList = Arrays.asList("a", "b", "c", "d", "e");
        DataStreamSource<String> fromCollection = env.fromCollection(dataList).setParallelism(1);//本算子是一个并行度的算子,尽管设置了并行度5，webui上可以看到该算子的并行度依然是1
        fromCollection.map(String::toUpperCase)/*.print()*/;

        //fromParallelCollectiond返回的source算子，是一个多并行的的算子

        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class)).setParallelism(2);
        parallelCollection.map(s->s.getValue()+100)/*.print()*/;

        //生成一个序列
        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        sequence.map(x->x-1)/*.print()*/;

        /**
         * 从文件中得到数据流
         * */
        DataStreamSource<String> fileSource = env.readTextFile("G:\\JavaProgram\\flink_duoyi_example\\data\\wc\\input\\wc.txt","utf-8");
        fileSource.map(String::toUpperCase)/*.print()*/;


        //FileProcessingMode.PROCESS_CONTINUOUSLY 表示，会检视这文件的变化，一旦文件发生变化，则会再次对整个文件进行重新计算
        //FileProcessingMode.PROCESS_ONCE 表示，对文件只读一次，计算一次，然后程序就退出
        DataStreamSource<String> fileSource2 = env.readFile(new TextInputFormat(null), "G:\\JavaProgram\\flink_duoyi_example\\data\\wc\\input\\wc.txt", FileProcessingMode.PROCESS_ONCE, 1000);
        fileSource2.map(String::toUpperCase)/*.print()*/;

        /**
         * 从kafka读取数据作为数据源
         * */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("tp01")
                .setGroupId("gp01")
                .setBootstrapServers("192.168.137.131:9092")
                //OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)消费起始位移选择之前所提交的偏移量，（如果没有，则重置LATEST）
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //OffsetsInitializer.earliest() 消费起始位置直接选择为“最早”
                //OffsetsInitializer.latest() 消费起始位置直接选择为"最新"
                //OffsetsInitializer.offsets(Map<TopicPartition,Long>)消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())

                //开启了kafka底层消费者的自动位移提交机制，它会吧最新的消费位移提交到kafka的consumer_offsets中
                //就算把自动位移提交机制开启，KafkaSource依然不会依赖自动位移提交机制（宕机重启时，优先从flink自己的状态中去获取偏移量《更可靠》）
                .setProperty("auto.offset.commit", "true")
                .build();
                // 如果只是变成String，就调用SimpleStringSchema()即可，以下等同
                        /*.setValueOnlyDeserializer(new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                return new String(bytes);
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        })*/
                //把本source算子设置成，BOUNDED属性（有界流），将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                //参数为结束位移
                //常用于补数或者重跑某一段历史数据
                // .setBounded(OffsetsInitializer.committedOffsets())
                //把本算子设置成UNBOUNDED属性（无界流），但是并不会一直的数据，而是达到指定位置就停止读取，但程序不退出
                //主要应用场景：需要从kafka中读取某一个固定长度的数据，然后拿着这段数据去另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())
//        env.addSource();//接受的是sourceFunction接口的实现类
//        env.fromSource(KafkaSource,);//接收的Source的接口的实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        streamSource.print();


        env.execute();

    }
}
