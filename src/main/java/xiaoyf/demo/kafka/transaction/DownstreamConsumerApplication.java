package xiaoyf.demo.kafka.transaction;

import org.apache.kafka.clients.consumer.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static xiaoyf.demo.kafka.transaction.Constants.*;

public class DownstreamConsumerApplication {
    private static void log(Object msg) {
        String strDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        System.out.println(strDate + ": " + msg);
    }

    private static Consumer<String, String> createConsumer() {
        Properties consumerProps = new Properties();

        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(GROUP_ID_CONFIG, "group3");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("isolation.level", "read_committed");

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(UPPERCASE_TOPIC));

        return consumer;
    }

    public static void main(String[] args) throws Exception {
        Consumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(500));

            log("Downstream Consumer received " + records.count() + " messages");

            for (ConsumerRecord<String, String> record : records) {
                log("Processing: (" + record.partition() + "/" + record.offset() + ") " + record.timestamp() + "/" + record.key() + "/" + record.value());
            }

            consumer.commitSync();
        }

    }
}