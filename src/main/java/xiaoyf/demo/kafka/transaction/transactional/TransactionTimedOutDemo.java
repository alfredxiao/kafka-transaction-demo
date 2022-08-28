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

@Slf4j
public class TransactionTimedOutDemo {
    final static long TEN_SECONDS = 10000L;

    public static void main(String[] args) throws Exception {
        // demoTimeoutWithoutCommitting();
        // demoCommitAfterTimedOut();
        // demoSendingAfterTimedOut();
        demoAbortingAfterTimedOut();
    }

    private static void demoTimeoutWithoutCommitting() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        // timed-out-demo::TransactionMetadata(transactionalId=timed-out-demo, producerId=3, producerEpoch=0, txnTimeoutMs=60000, state=Empty, pendingState=None, topicPartitions=HashSet(), txnStartTimestamp=-1, txnLastUpdateTimestamp=1661647168231)
        producer.beginTransaction();
        // first message marks the starting timestamp of a transaction
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "demoExitWithoutCommitting")).get();
        // timed-out-demo::TransactionMetadata(transactionalId=timed-out-demo, producerId=3, producerEpoch=0, txnTimeoutMs=60000, state=Ongoing, pendingState=None, topicPartitions=HashSet(transaction-demo-0), txnStartTimestamp=1661647178282, txnLastUpdateTimestamp=1661647178282)
        Thread.sleep(8 * TEN_SECONDS);

        // NOTE: a little more than 60 seconds after the starting timestamp of the transaction, it is marked (on broker side) to be aborted by a bumped epoch number!
        // timed-out-demo::TransactionMetadata(transactionalId=timed-out-demo, producerId=3, producerEpoch=1, txnTimeoutMs=60000, state=PrepareAbort, pendingState=None, topicPartitions=HashSet(transaction-demo-0), txnStartTimestamp=1661647178282, txnLastUpdateTimestamp=1661647246576)
        // timed-out-demo::TransactionMetadata(transactionalId=timed-out-demo, producerId=3, producerEpoch=1, txnTimeoutMs=60000, state=CompleteAbort, pendingState=None, topicPartitions=HashSet(), txnStartTimestamp=1661647178282, txnLastUpdateTimestamp=1661647246577)
    }

    private static void demoCommitAfterTimedOut() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k2", "demoCommitAfterTimedOut")).get();
        Thread.sleep(8 * TEN_SECONDS);

        producer.commitTransaction();
        // ProducerFencedException! because tx has been aborted by broker side with bumped epoch
    }

    private static void demoSendingAfterTimedOut() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k3", "demoSendingAfterTimedOut1")).get();
        Thread.sleep(8 * TEN_SECONDS);

        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k4", "demoSendingAfterTimedOut2")).get();
        // InvalidProducerEpochException: Producer attempted to produce with an old epoch.
    }

    private static void demoAbortingAfterTimedOut() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k5", "demoAbortingAfterTimedOut")).get();
        Thread.sleep(8 * TEN_SECONDS);

        producer.abortTransaction();
        // ProducerFencedException: There is a newer producer with the same transactionalId which fences the current one.
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "timed-out-demo");

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. Timed out transactions are aborted on broker side
  2. Producer sees ProducerFencedException when trying to commit or abort the transaction that has been aborted by broker
  3. Producer sees InvalidProducerEpochException when trying to send records within the aborted transaction
 */