package kafka2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Kafka2UpsertKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        kafka2UpsertKafka(tableEnvironment);

    }

    public static void kafka2UpsertKafka(StreamTableEnvironment tableEnvironment) throws Exception {
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
                "  cnt BIGINT" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'group_data',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'csv')";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySql = "insert into csvData \n" +
                "select user_name, is_new, count(content) from\n" +
                "csv group by  user_name, is_new";
        tableEnvironment.sqlUpdate(querySql);

        tableEnvironment.execute("kafka2UpsertKafka");
    }

}
