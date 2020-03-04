package kafka2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class KafkaCsv2Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

//        flinkFileCsv2KafkaCsv(tableEnvironment);
        flinkKafkaCsv2Csv(tableEnvironment);
    }

    public static void flinkFileCsv2KafkaCsv(StreamTableEnvironment tableEnvironment) throws Exception{
        String csvSourceDDL = "create table csv(" +
                " user_name VARCHAR," +
                " is_new BOOLEAN," +
                " content VARCHAR" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',\n" +
                " 'format.type' = 'csv',\n" +
                " 'format.fields.0.name' = 'user_name',\n" +
                " 'format.fields.0.data-type' = 'STRING',\n" +
                " 'format.fields.1.name' = 'is_new',\n" +
                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
                " 'format.fields.2.name' = 'content',\n" +
                " 'format.fields.2.data-type' = 'STRING')";
        tableEnvironment.sqlUpdate(csvSourceDDL);

        String sinkTableDDL = "CREATE TABLE csvData (\n" +
                "  user_name STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING" +
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
                "select user_name, is_new, content from\n" +
                "csv";
        tableEnvironment.sqlUpdate(querySql);
        System.out.println(csvSourceDDL);
        System.out.println(sinkTableDDL);
        System.out.println(querySql);
        tableEnvironment.execute("flinkFileCsv2KafkaCsv");
    }

    public static void flinkKafkaCsv2Csv(StreamTableEnvironment tableEnvironment) throws Exception {
        String soureTableDDL = "CREATE TABLE csvData (\n" +
                "  user_name STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'csv_data',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup4',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'csv')";
        tableEnvironment.sqlUpdate(soureTableDDL);
        String csvSinkDDL = "create table csv(" +
                " user_name VARCHAR," +
                " is_new BOOLEAN," +
                " content VARCHAR" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user2.csv',\n" +
                " 'format.type' = 'csv',\n" +
                " 'update-mode' = 'append', \n" +
                " 'format.fields.0.name' = 'user_name',\n" +
                " 'format.fields.0.data-type' = 'STRING',\n" +
                " 'format.fields.1.name' = 'is_new',\n" +
                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
                " 'format.fields.2.name' = 'content',\n" +
                " 'format.fields.2.data-type' = 'STRING')";
        tableEnvironment.sqlUpdate(csvSinkDDL);

        String querySql = "insert into csv select user_name, is_new, content from csvData";

        tableEnvironment.sqlUpdate(querySql);
        System.out.println(soureTableDDL);
        System.out.println(csvSinkDDL);
        System.out.println(querySql);
        tableEnvironment.execute("flinkKafkaCsv2Csv");
    }


}
