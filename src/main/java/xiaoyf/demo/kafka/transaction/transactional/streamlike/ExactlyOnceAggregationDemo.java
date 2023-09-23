package xiaoyf.demo.kafka.transaction.transactional.streamlike;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;

/**
 * ExactlyOnceAggregationDemo demonstrates a time interval, or schedule based aggregation algorithm works
 * in Kafka consumer/producer setting.
 */
@Slf4j
public class ExactlyOnceAggregationDemo {
    private final static String GROUP_ID = "exactly-once-aggregation-v2";

    public static void main(String[] args) throws Exception {

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(singleton("t1"));

        producer.initTransactions();

        long interval = Duration.ofSeconds(20).toMillis();
        long lastCheckPoint = System.currentTimeMillis();

        List<ConsumerRecord<String,String>> recordBatch = new ArrayList<>();
        Map<TopicPartition, OffsetAndMetadata> offsetBatch = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(recordBatch::add);
            offsetBatch.putAll(currentOffsets(records));

            if (System.currentTimeMillis() - lastCheckPoint >= interval) {

                if (recordBatch.size() > 0) {
                    String aggregatedKeys =
                            recordBatch.stream().map(ConsumerRecord::key).collect(Collectors.joining(","));
                    String aggregatedValues =
                            recordBatch.stream().map(ConsumerRecord::value).collect(Collectors.joining(","));

                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>("t2", aggregatedKeys, aggregatedValues));
                    producer.sendOffsetsToTransaction(offsetBatch, GROUP_ID);

                    log.info("Commit Tx: {}", aggregatedValues);
                    producer.commitTransaction();
                }

                recordBatch.clear();
                offsetBatch.clear();
                lastCheckPoint = System.currentTimeMillis();
            }
        }

    }

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }

        return offsetsToCommit;
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(TRANSACTIONAL_ID_CONFIG, "exactly-once-aggregation");
        producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(producerProps);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        producerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        producerProps.put(GROUP_ID_CONFIG, GROUP_ID);
        producerProps.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        producerProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(producerProps);
    }

}