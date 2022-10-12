package flinksql.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo2_TableApi {
    public static void main(String[] args) {
        //纯粹的表环境
//        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        //混合环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //建表
        Table table = tableEnv.from(TableDescriptor
                .forConnector("kafka")//指定连接器
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")
                .option("topic", "test")
                .option("properties.bootstrap.servers", "hadoop01:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mpde", "earlist-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());

        //查询
        Table table2 = table.groupBy($("gender"))
                .select($("gender"), $("age").avg().as("avg_age"));

        /*
        * 将一个已创建好的table对象，注册成sql中的视图名
        * */


        tableEnv.createTemporaryView("kafka_table",table);
        //然后就可以写sql语句来进行查询了
        tableEnv.executeSql("select gender,avg(age) as avg_age from kafka_table group by gender").print();


        //输出
        table2.execute().print();

        //
    }
}
