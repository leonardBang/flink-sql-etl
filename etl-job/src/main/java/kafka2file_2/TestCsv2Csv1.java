package kafka2file_2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestCsv2Csv1 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);

        String csvSourceDDL = "create table csv(" +
                "rowkey INT,\n" +
                "f1c1 INT,\n" +
                "f2c1 STRING,\n" +
                "f2c2 BIGINT,\n" +
                "f3c1 DOUBLE,\n" +
                "f3c2 BOOLEAN,\n" +
                "f3c3 STRING,\n" +
                "f4c1 TIMESTAMP(3),\n" +
                "f4c2 DATE,\n" +
                "f4c3 TIME(3),\n" +
                "f5c1 TIMESTAMP(4),\n" +
                "f5c2 DECIMAL(10, 4)" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/Improve/flinkstream/src/main/resources/test1.csv',\n" +
                " 'format.type' = 'csv'" +
                ")";
        tableEnvironment.sqlUpdate(csvSourceDDL);
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery("select f5c1, f5c2 from csv"), Row.class).print();

        tableEnvironment.execute("csvTest");

    }

}
