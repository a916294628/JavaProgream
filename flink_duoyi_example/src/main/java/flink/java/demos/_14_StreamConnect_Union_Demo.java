package flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *@author AMe
 *@date 2022/8/28 21:31
 *@describe:
 *流的连接connect算子  及   流的关联join算子  代码示例
 */
public class _14_StreamConnect_Union_Demo {
    public static void main(String[] args) throws Exception {
        //开启一个本地Flinkweb
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt/");


        //数字字符流
        DataStreamSource<String> stream1 = env.socketTextStream("192.168.137.131", 9997);


        //字母字符流
        DataStreamSource<String> stream2 = env.socketTextStream("192.168.137.131", 9998);


        //流的connect

        ConnectedStreams<String, String> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<String, String, String>() {

            //共同的数据状态
            String prefix = "wxf_";
            /**
             * @Description:
             *对左流的处理逻辑
             * @param: s
             * @return: java.lang.String
             * @Author: hello
             * @Date 2022/8/29 22:41
             */

            @Override
            public String map1(String s) throws Exception {
                return prefix + (Integer.parseInt(s) * 10) + "";
            }
            /**
             * @Description:
             *对右流的处理逻辑
             * @param: s
             * @return: java.lang.String
             * @Author: hello
             * @Date 2022/8/29 22:41
             */

            @Override
            public String map2(String s) throws Exception {
                return prefix + s.toUpperCase();
            }
        });
//        resultStream.print();

        /*
        * 流的union
        * 参与的流的数据类型必须一致
        * */
        DataStream<String> unioned = stream1.union(stream2);
        unioned.map(s->"wxf_"+s).print();

        env.execute();


    }
}
