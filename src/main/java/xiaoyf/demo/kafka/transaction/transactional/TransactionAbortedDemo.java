package xiaoyf.demo.kafka.transaction.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.TransactionManager;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.TRANSACTION_DEMO_TOPIC;

@Slf4j
public class TransactionAbortedDemo {
    final static long TEN_SECONDS = 10000L;

    public static void main(String[] args) throws Exception {
        // abortAbortedTransaction();
        sendAfterAbort();
    }

    private static void normalAbort() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "transaction-aborted-message1")).get();
        producer.abortTransaction();
    }

    private static void abortAbortedTransaction() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "transaction-aborted-message2")).get();
        producer.abortTransaction();
        producer.abortTransaction();
        // KafkaException: TransactionalId tx-abort-demo: Invalid transition attempted from state READY to state ABORTING_TRANSACTION
    }

    private static void sendAfterAbort() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "transaction-aborted-message3")).get();
        producer.abortTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k2", "transaction-aborted-message4"));
        // KafkaException: TransactionalId tx-abort-demo: Invalid transition attempted from state READY to state ABORTING_TRANSACTION
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-abort-demo");

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. Although transaction is aborted, records are still appended to topic data log file, seen by consumers with
     read_uncommitted
  2. An ABORT marker is appended to topic data log file
    baseOffset: 22 lastOffset: 22 count: 1 baseSequence: 0 lastSequence: 0 producerId: 5 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 1896 CreateTime: 1661650185531 size: 98 magic: 2 compresscodec: NONE crc: 2864438883 isvalid: true
        | offset: 22 isValid: true crc: null keySize: 2 valueSize: 28 CreateTime: 1661650185531 baseOffset: 22 lastOffset: 22 baseSequence: 0 lastSequence: 0 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 98 magic: 2 compressType: NONE position: 1896 sequence: 0 headerKeys: [] key: k1 payload: transaction-aborted-message1
    baseOffset: 23 lastOffset: 23 count: 1 baseSequence: -1 lastSequence: -1 producerId: 5 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: true position: 1994 CreateTime: 1661650185562 size: 78 magic: 2 compresscodec: NONE crc: 2431180806 isvalid: true
        | offset: 23 isValid: true crc: null keySize: 4 valueSize: 6 CreateTime: 1661650185562 baseOffset: 23 lastOffset: 23 baseSequence: -1 lastSequence: -1 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 78 magic: 2 compressType: NONE position: 1994 sequence: -1 headerKeys: [] endTxnMarker: ABORT coordinatorEpoch: 0
 */