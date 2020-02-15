package kafka2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class KafkaAvro2Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        // step 1, should note step 2 when call
        //        flinkCsv2Avro(tableEnvironment);

        // step 2, should note step step 1 when call
        flinkAvro2Avro(tableEnvironment);

    }

    private static void flinkCsv2Avro(StreamTableEnvironment tableEnvironment) throws Exception{
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

        tableEnvironment.sqlUpdate("CREATE TABLE AvroTest (\n" +
                "  user_name VARCHAR,\n" +
                "  is_new BOOLEAN,\n" +
                "  content VARCHAR" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'avro_from_csv',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'avro',\n" +
                "  'format.record-class' = 'kafka.UserAvro'\n" +
                ")\n");
        String querySQL = "insert into AvroTest select user_name, is_new, content from csv ";
        tableEnvironment.sqlUpdate(querySQL);
        tableEnvironment.execute("FlinkCsv2Avro");
    }

    private static void flinkAvro2Avro(StreamTableEnvironment tableEnvironment) throws Exception{
        String sinkTableDDL = "CREATE TABLE WikipediaFeed_filtered (\n" +
                "  user_name STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'WikipediaFeed2_filtered',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'avro',\n" +
                "  'format.avro-schema' =\n" +
                "    '{ \n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"UserAvro\",\n" +
                "    \"fields\": [\n" +
                "      {\"name\": \"user_name\", \"type\": \"string\"},\n" +
                "      {\"name\": \"is_new\", \"type\": \"boolean\"},\n" +
                "      {\"name\": \"content\", \"type\": \"string\"}\n" +
                "      ]\n" +
                "    }'" +
                ")\n";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "insert into WikipediaFeed_filtered \n" +
                "select user_name, is_new, content \n" +
                "from AvroTest\n" ;

        tableEnvironment.sqlUpdate(querySQL);
        tableEnvironment.execute("FlinkAvro2Avro");
    }
}
