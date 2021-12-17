package xiaoyf.demo.kafka.transaction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static xiaoyf.demo.kafka.transaction.Constants.*;

public class Application {
    private static void log(Object msg) {
        String strDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        System.out.println(strDate + ": " + msg);
    }

    private static Consumer<String, String> createConsumer() {
        Properties consumerProps = new Properties();

        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(GROUP_ID_CONFIG, "group1");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("isolation.level", "read_committed");

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(LOWERCASE_TOPIC));

        return consumer;
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put("enable.idempotence", "true");
        producerProps.put("transactional.id", "prod-2");

        return new KafkaProducer<>(producerProps);
    }

    private static KafkaProducer<String, String> createAnotherProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        return new KafkaProducer<>(producerProps);
    }

    private static String upper(String lower) {
        return Objects.isNull(lower) ? null : lower.toUpperCase();
    }


    public static void main(String[] args) throws Exception {
        Consumer<String, String> consumer = createConsumer();
        KafkaProducer<String, String> producer = createProducer();
        KafkaProducer<String, String> anotherProducer = createAnotherProducer();

        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofSeconds(10));

            log("Consumer received " + records.count() + " messages");

            for (ConsumerRecord<String, String> record : records) {
                log("Processing: " + record.key() + "/" + record.value());
                producer.beginTransaction();

                producer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value())));
                Thread.sleep(100);
                log("Send uppercase:" + upper(record.value()));

                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                producer.sendOffsetsToTransaction(offsetsToCommit, GROUPD_ID_1);
//                producer.abortTransaction();

                anotherProducer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " 1 by another producer"));
                anotherProducer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " 2 by another producer"));
                anotherProducer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " 3 by another producer"));
                anotherProducer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " 4 by another producer"));
                anotherProducer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " 5 by another producer"));
                anotherProducer.flush();

                Thread.sleep(150);
                producer.send(new ProducerRecord<>(UPPERCASE_TOPIC, upper(record.key()), upper(record.value()) + " again"));

                Thread.sleep(2500);

                producer.commitTransaction();
                log("uppercase record committed");
            }
        }

//        try {
//            producer.send(record);
//        } catch(SerializationException e) {
//            e.printStackTrace();
//        } finally {
//            producer.flush();
//            producer.close();
//        }
    }
}