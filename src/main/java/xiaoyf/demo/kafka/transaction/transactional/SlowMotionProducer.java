package xiaoyf.demo.kafka.transaction.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.TRANSACTION_DEMO_TOPIC;

/**
 * SimpleTransactionSlowMotionProducer demonstrate corresponding interaction with broker side (by comments though :).
 * Tools used to monitor what's happening on broker side:
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

        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k0", "slow-motion-message")).get();
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=Ongoing, pendingState=None, topicPartitions=HashSet(transaction-demo-0), txnStartTimestamp=1661605715280, txnLastUpdateTimestamp=1661605715280)
        // record is observed by consumer(read_uncommitted), but not by consumer(read_committed)
        Thread.sleep(TEN_SECOND);

        producer.commitTransaction(); // -> Ongoing, PrepareCommit, CompleteCommit
        // slow-motion::TransactionMetadata(transactionalId=slow-motion, producerId=30, producerEpoch=3, txnTimeoutMs=60000, state=PrepareCommit, pendingState=None, topicPartitions=HashSet(transaction-demo-0), txnStartTimestamp=1661605715280, txnLastUpdateTimestamp=1661605725413)
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

/* NOTE
  1. beginTransaction() does not cause anything to be done on broker side
  2. send() does append records in topic data log file, no matter transaction is committed or not
  3. On __transaction_state topic
    - state=Empty           -> a producer is registered
    - state=Ongoing         -> a transaction has started
    - state=PrepareCommit   -> This update is considered the synchronization barrier of the transaction: once the state update is replicated in the transaction log, there is no turning back.
                               The transaction is guaranteed to be committed even if the transaction coordinator crashes immediately after.
                               The coordinator can then begin the second phase asynchronously, where it writes a transaction commit marker to each of the transaction’s registered partitions.
    - state=CompleteCommit  -> After all the transaction markers have been acked by the partition leaders, the transaction coordinator updates its transaction state to CompleteCommit, which allows the producer client to start a new transaction.
 */