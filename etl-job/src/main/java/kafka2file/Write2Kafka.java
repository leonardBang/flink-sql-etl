package kafka2file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Write2Kafka {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);
        constructKafkaData(tableEnvironment);

    }

    private static void constructKafkaData(StreamTableEnvironment tableEnvironment) throws Exception {
        String csvSourceDDL = "create table csv( " +
                "user_name VARCHAR, " +
                "is_new BOOLEAN, " +
                "content VARCHAR, " +
                "date_col VARCHAR) with ( " +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user_part.csv',\n" +
                " 'format.type' = 'csv',\n" +
                " 'format.fields.0.name' = 'user_name',\n" +
                " 'format.fields.0.data-type' = 'STRING',\n" +
                " 'format.fields.1.name' = 'is_new',\n" +
                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
                " 'format.fields.2.name' = 'content',\n" +
                " 'format.fields.2.data-type' = 'STRING',\n" +
                " 'format.fields.3.name' = 'date_col',\n" +
                " 'format.fields.3.data-type' = 'STRING')";
        tableEnvironment.sqlUpdate(csvSourceDDL);

        String sinkTableDDL = "CREATE TABLE csvData (\n" +
                "  user_name STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING,\n" +
                "  date_col STRING" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'csv_data',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'csv')";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySql = "insert into csvData \n" +
                "select user_name, is_new, content, date_col from\n" +
                "csv";
        tableEnvironment.sqlUpdate(querySql);
        tableEnvironment.execute("flinkFileCsv2KafkaCsv");
    }
}
