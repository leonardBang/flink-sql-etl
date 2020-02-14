package kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaGenerator {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, "0");
        kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        kafkaProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "163840");
        kafkaProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "100000");

//        Thread thread1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    CurrencySender.sendMessage(kafkaProperties, 1);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        thread1.start();

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    JsonOrderSender.sendMessage(kafkaProperties,  3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
       thread2.start();
    }
}
