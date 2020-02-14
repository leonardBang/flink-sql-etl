package kafka2kafka_1;

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

        tableEnvironment.sqlUpdate("CREATE TABLE WikipediaFeed (\n" +
                "  `user` STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'WikipediaFeed',\n" +
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
                "      {\"name\": \"user\", \"type\": \"string\"},\n" +
                "      {\"name\": \"is_new\", \"type\": \"boolean\"},\n" +
                "      {\"name\": \"content\", \"type\": [\"string\", \"null\"]}\n" +
                "      ]\n" +
                "    }'" +
                ")\n");

        String sinkTableDDL = "CREATE TABLE WikipediaFeed_filtered (\n" +
                "  `user` STRING,\n" +
                "  is_new    BOOLEAN,\n" +
                "  content STRING" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'WikipediaFeed_filtered',\n" +
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
                "      {\"name\": \"user\", \"type\": \"string\"},\n" +
                "      {\"name\": \"is_new\", \"type\": \"boolean\"},\n" +
                "      {\"name\": \"content\", \"type\": [\"string\", \"null\"]}\n" +
                "      ]\n" +
                "    }'" +
                ")\n";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "insert into WikipediaFeed_filtered \n" +
                "select `user`, is_new, content \n" +
                "from WikipediaFeed\n" +
                "where `user` in ('phil', 'damian', 'lauren')\n" ;

        tableEnvironment.sqlUpdate(querySQL);
        tableEnvironment.execute("KafkaCsv2Kafka");
    }
}
