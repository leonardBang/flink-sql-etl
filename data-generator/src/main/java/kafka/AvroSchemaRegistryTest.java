package kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class AvroSchemaRegistryTest {
    public static final String WIKIPEDIA_FEED = "WikipediaFeed2_filtered";

    public static void main(final String[] args) throws IOException {
        produceInputs();
        consumeOutput();
    }

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
                            producer.send(new ProducerRecord<>(WIKIPEDIA_FEED, record.getUserName(), record));
                        });

        producer.flush();
    }

    private static void consumeOutput() {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put("schema.registry.url", "http://localhost:8081");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-feed-example-consumer3");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<String, UserAvro> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singleton(WIKIPEDIA_FEED));
        while (true) {
            final ConsumerRecords<String, UserAvro> consumerRecords = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, UserAvro> consumerRecord : consumerRecords) {

                System.out.println(consumerRecord.key() + "=" + consumerRecord.value());
            }
        }
    }

}
