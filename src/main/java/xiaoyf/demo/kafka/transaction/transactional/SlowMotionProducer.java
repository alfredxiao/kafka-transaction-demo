package xiaoyf.demo.kafka.transaction.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.Constants.SINGLE_TRANSACTIONAL_PRODUCER_TOPIC;

/**
 * SimpleTransactionSlowMotionProducer demonstrate corresponding interaction with broker side (by comments though :).
 * Tools used to monitor what's happending on broker side:
 * <code>kafka-console-consumer --consumer-property "exclude.internal.topics":"false" --topic __transaction_state --bootstrap-server localhost:9092 --formatter "kafka.coordinator.transaction.TransactionLog\$TransactionLogMessageFormatter"</code>
 * <code>kafka-console-consumer --consumer-property "isolation.level"="read_uncommitted" --topic single-transactional-producer --bootstrap-server localhost:9092</code>
 * <code>kafka-console-consumer --consumer-property "isolation.level"="read_committed" --topic single-transactional-producer --bootstrap-server localhost:9092</code>
 * <code>kafka-dump-log --files 00000000000000000000.log --print-data-log</code>
 */
@Slf4j
public class SlowMotionProducer {
    static final long TEN_SECOND = 10000L;

    public static void main(String[] args) throws Exception {

        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=Empty, pendingState=None, topicPartitions=HashSet(), txnStartTimestamp=-1, txnLastUpdateTimestamp=1661605695070
        Thread.sleep(TEN_SECOND);

        producer.beginTransaction();
        Thread.sleep(TEN_SECOND);

        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v1")).get();
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=Ongoing, pendingState=None, topicPartitions=HashSet(single-transactional-producer-0), txnStartTimestamp=1661605715280, txnLastUpdateTimestamp=1661605715280)
        // record is observed by consumer(read_uncommitted), but not by consumer(read_committed)
        Thread.sleep(TEN_SECOND);

        producer.commitTransaction(); // -> Ongoing, PrepareCommit, CompleteCommit
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=PrepareCommit, pendingState=None, topicPartitions=HashSet(single-transactional-producer-0), txnStartTimestamp=1661605715280, txnLastUpdateTimestamp=1661605725413)
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=CompleteCommit, pendingState=
        // record is observed by consumer(read_committed)
        // kafka-dump-log sees a COMMIT marker on the topic
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "slow-motion");

        return new KafkaProducer<>(producerProps);
    }

}
