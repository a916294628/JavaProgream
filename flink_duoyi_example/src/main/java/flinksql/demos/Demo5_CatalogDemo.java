package flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo5_CatalogDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.createTable("table_e",
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name",DataTypes.STRING())
                                .column("agr",DataTypes.INT())
                                .column("gender",DataTypes.STRING())
                                .build())
                        .format("csv")
                        .option("path","fire:///G:\\JavaProgram\\flink_duoyi_example\\data\\sqldemo")
                        .option("csv.ignore-parse-errors","true")
                        .option("csv.allow-comments","true")
                        .build());
    }
}
