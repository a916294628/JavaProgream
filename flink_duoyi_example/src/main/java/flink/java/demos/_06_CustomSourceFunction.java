package flink.java.demos;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

/*
* 自定义的source算子,可以实现SourceFunction，RichSourceFunction 这两者非并行的source算子
* 也可实现  parallelSourceFunction RichParallelSourceFunction  这两者并行的source算子
*
* ---带rich的，都拥有open(),close(),getRuntimeContext() 方法
* ---带Parallen 都可以设置并行度
*
*
* */
public class _06_CustomSourceFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStreamSource<EventLog> dataStreamSource = env.addSource(new MySourceFunction());//都是不能并行的
        DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichSourceFunction());//都是不能并行的
        //DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyParallelSourceFunction()).setParallelism(2);
        //DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichParallelSourceFunction()).setParallelism(2);

        //dataStreamSource.map(eventLog -> JSON.toJSON(eventLog)).print();
        dataStreamSource.map(JSON::toJSONString).print();
        env.execute();


    }
}


/*
* 不可以并行的
* */

class MySourceFunction implements SourceFunction<EventLog>{
    volatile boolean flag = true;
    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {

        EventLog eventLog = new EventLog();

        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();
        while(flag){
            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);
            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            sourceContext.collect(eventLog);
            eventInfoMap.clear();
//            Thread.sleep(RandomUtils.nextInt(500,1500));

            Thread.sleep(RandomUtils.nextInt(200,1500));
        }

    }

    @Override
    public void cancel() {
        flag = false;

    }
}

/*
* 可以并行的，可以在读源时设置并行度
* */
class MyParallelSourceFunction implements ParallelSourceFunction<EventLog>{
    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
/*
* 不可以并行的
* */
class MyRichSourceFunction extends RichSourceFunction<EventLog>{
    boolean flag = true;
    /*
    * source组件的初始化
    * */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext runtimeContext = getRuntimeContext();
        //可以从运行时上下文中，取得本算子所属的task的task名
        String taskName = runtimeContext.getTaskName();
        //可以从运行时上下文中，取得本算子所属的subtask的subTaskId
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();


    }

    /*
    * source组件生成数据的额过程（核心工作逻辑）
    * */

    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {
        EventLog eventLog = new EventLog();

        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();
        while(flag){
            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);
            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            sourceContext.collect(eventLog);
            eventInfoMap.clear();
            Thread.sleep(RandomUtils.nextInt(500,1500));

        }

    }
    /*
    * job取消时调用的方法
    * */

    @Override
    public void cancel() {
        flag = false;

    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("组件被关闭了。。。。。");
    }
}

/*
* 可以并行的，可以在读源时设置并行度
* */
class MyRichParallelSourceFunction extends RichParallelSourceFunction<EventLog>{
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}



