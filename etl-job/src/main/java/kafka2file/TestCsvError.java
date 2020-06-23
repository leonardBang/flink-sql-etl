package kafka2file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestCsvError {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);

        String csvSourceDDL = "CREATE TABLE `src` (\n" +
            "key bigint,\n" +
            "v varchar\n" +
            ") WITH (\n" +
            "'connector'='filesystem',\n" +
            "'csv.field-delimiter'='|',\n" +
            "'path'='/Users/bang/src.csv',\n" +
            "'csv.null-literal'='',\n" +
            "'format'='csv'\n" +
            ")";
        String csvSinkDDL = "CREATE TABLE `sink` (\n" +
            "c1 decimal(10, 2),\n" +
            "c2 varchar,\n" +
            "c3 varchar" +
            ") WITH (\n" +
            "'connector'='filesystem',\n" +
            "'csv.field-delimiter'='|',\n" +
            "'path'='/Users/bang/sink.csv',\n" +
            "'csv.null-literal'='',\n" +
            "'format'='csv'\n" +
            ")";

        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.executeSql(csvSinkDDL);
//        tableEnvironment.executeSql("insert into sink select\n" +
//            " cast(key as decimal(10,2)) as c1,\n" +
//            " cast(key as char(10)) as c2,\n" +
//            " cast(key as varchar(10)) as c3\n" +
//            " from src\n").collect();

        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select\n" +
            " cast(key as decimal(10,2)) as c1,\n" +
                " cast(key as char(10)) as c2,\n" +
                " cast(key as varchar(10)) as c3\n" +
                " from src\n"), Row.class).print();
        executionEnvironment.execute("csvTest");
    }

}
