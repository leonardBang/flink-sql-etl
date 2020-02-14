package kafka2kafka_1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.UserAvro;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class ConsumeConfluentAvroTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

        String tableDDL = "CREATE TABLE WikipediaFeed (\n" +
                "  user_name STRING,\n" +
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
                "      {\"name\": \"user_name\", \"type\": \"string\"},\n" +
                "      {\"name\": \"is_new\", \"type\": \"boolean\"},\n" +
                "      {\"name\": \"content\", \"type\": \"string\"}\n" +
                "      ]\n" +
                "    }'" +
                ")\n";
        tableEnvironment.sqlUpdate(tableDDL);

        String querySQL = "select user_name, is_new, content \n" +
                "from WikipediaFeed\n" ;
        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(querySQL), Row.class).print();
        tableEnvironment.execute("KafkaAvro2Kafka");
    }

    // prepare confluent avro foramt data
    private static void produceInputs() throws IOException {
        final String[] users = {"leonard", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        final KafkaProducer<String, UserAvro> producer = new KafkaProducer<>(props);
        final Random random = new Random();

        IntStream.range(0, 10)
                .mapToObj(value -> new UserAvro(users[random.nextInt(users.length)], true, "content"))
                .forEach(
                        record -> {
                            System.out.println(record.toString()) ;
                            producer.send(new ProducerRecord<>("WikipediaFeed", record.getUserName(), record));
                        });

        producer.flush();
    }
}
