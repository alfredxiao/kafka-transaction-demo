package xiaoyf.demo.kafka.transaction.transactional.streamlike;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.OFFSET_DEMO_INPUT_TOPIC;
import static xiaoyf.demo.kafka.transaction.helper.Constants.OFFSET_DEMO_OUTPUT_TOPIC;

public class TransactionalOffsetDemo {
    private final static String GROUP_ID = "offset-demo-consumer-group";

    public static void main(String[] args) throws Exception {

        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(singleton(OFFSET_DEMO_INPUT_TOPIC));

        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) continue;

            producer.beginTransaction();
            for (ConsumerRecord<String, String> record : records) {
                producer.send(new ProducerRecord<>(OFFSET_DEMO_OUTPUT_TOPIC, record.key(), record.value() + ".processed"));
            }

            Thread.sleep(3000);
            producer.sendOffsetsToTransaction(currentOffsets(records), GROUP_ID);
            producer.commitTransaction();
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
        producerProps.put(TRANSACTIONAL_ID_CONFIG, "offset-tx");

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
/* transaction__state log
offset-tx::TransactionMetadata(transactionalId=offset-tx, producerId=2009, producerEpoch=1, txnTimeoutMs=60000, state=Empty, pendingState=None, topicPartitions=HashSet(), txnStartTimestamp=-1, txnLastUpdateTimestamp=1663462902446)
offset-tx::TransactionMetadata(transactionalId=offset-tx, producerId=2009, producerEpoch=1, txnTimeoutMs=60000, state=Ongoing, pendingState=None, topicPartitions=HashSet(offset-demo-output-0), txnStartTimestamp=1663462940166, txnLastUpdateTimestamp=1663462940166)
offset-tx::TransactionMetadata(transactionalId=offset-tx, producerId=2009, producerEpoch=1, txnTimeoutMs=60000, state=Ongoing, pendingState=None, topicPartitions=HashSet(offset-demo-output-0, __consumer_offsets-4), txnStartTimestamp=1663462940166, txnLastUpdateTimestamp=1663462943100)
offset-tx::TransactionMetadata(transactionalId=offset-tx, producerId=2009, producerEpoch=1, txnTimeoutMs=60000, state=PrepareCommit, pendingState=None, topicPartitions=HashSet(offset-demo-output-0, __consumer_offsets-4), txnStartTimestamp=1663462940166, txnLastUpdateTimestamp=1663462943298)
offset-tx::TransactionMetadata(transactionalId=offset-tx, producerId=2009, producerEpoch=1, txnTimeoutMs=60000, state=CompleteCommit, pendingState=None, topicPartitions=HashSet(), txnStartTimestamp=1663462940166, txnLastUpdateTimestamp=1663462943314)
 */
/* partition log
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: 0 lastSequence: 0 producerId: 2009 producerEpoch: 1 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 0 CreateTime: 1663462940087 size: 82 magic: 2 compresscodec: NONE crc: 2199391510 isvalid: true
| offset: 0 isValid: true crc: null keySize: 2 valueSize: 12 CreateTime: 1663462940087 baseOffset: 0 lastOffset: 0 baseSequence: 0 lastSequence: 0 producerEpoch: 1 partitionLeaderEpoch: 0 batchSize: 82 magic: 2 compressType: NONE position: 0 sequence: 0 headerKeys: [] key: k1 payload: V1.processed
baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: 2009 producerEpoch: 1 partitionLeaderEpoch: 0 isTransactional: true isControl: true position: 82 CreateTime: 1663462943443 size: 78 magic: 2 compresscodec: NONE crc: 709308939 isvalid: true
| offset: 1 isValid: true crc: null keySize: 4 valueSize: 6 CreateTime: 1663462943443 baseOffset: 1 lastOffset: 1 baseSequence: -1 lastSequence: -1 producerEpoch: 1 partitionLeaderEpoch: 0 batchSize: 78 magic: 2 compressType: NONE position: 82 sequence: -1 headerKeys: [] endTxnMarker: COMMIT coordinatorEpoch: 0
 */