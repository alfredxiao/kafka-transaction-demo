package xiaoyf.demo.kafka.transaction.nontransactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.IDEMPOTENT_TOPIC;
import static xiaoyf.demo.kafka.transaction.helper.Utils.nap;

@Slf4j
public class IdempotentProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        /*  producer: {
              clientId: producer-1,
              transactionManager: {
                currentState: INITIALIZING,
                transactionStarted: false,
                producerIdAndEpoch: {
                  producerId=-1, epoch=-1
                }
              }
           } */
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k1", "idempotent-message-1")).get();
        nap();
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k2", "idempotent-message-2")).get();
        /*  producer: {
              clientId: producer-1,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId=2005, epoch=0
                }
              }
           } */
        /* partition log file:
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: 0 lastSequence: 0 producerId: 2005 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1663408065534 size: 90 magic: 2 compresscodec: NONE crc: 3074630085 isvalid: true
| offset: 0 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408065534 baseOffset: 0 lastOffset: 0 baseSequence: 0 lastSequence: 0 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 90 magic: 2 compressType: NONE position: 0 sequence: 0 headerKeys: [] key: k1 payload: idempotent-message-1
baseOffset: 1 lastOffset: 1 count: 1 baseSequence: 1 lastSequence: 1 producerId: 2005 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 90 CreateTime: 1663408066142 size: 90 magic: 2 compresscodec: NONE crc: 1957593862 isvalid: true
| offset: 1 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408066142 baseOffset: 1 lastOffset: 1 baseSequence: 1 lastSequence: 1 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 90 magic: 2 compressType: NONE position: 90 sequence: 1 headerKeys: [] key: k2 payload: idempotent-message-2
            */
        producer.close();

        producer = createProducer();
        /*  producer: {
              clientId: producer-2,
              transactionManager: {
                currentState: INITIALIZING,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId: -1,
                  epoch: -1
                }
              }
           } */
        nap();
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k3", "idempotent-message-3")).get();
        /*  producer: {
              clientId: producer-2,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId: 2006,
                  epoch: 0
                }
              }
           } */
        /* partition log entry
baseOffset: 2 lastOffset: 2 count: 1 baseSequence: 0 lastSequence: 0 producerId: 2006 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 180 CreateTime: 1663408066744 size: 90 magic: 2 compresscodec: NONE crc: 3566985060 isvalid: true
| offset: 2 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408066744 baseOffset: 2 lastOffset: 2 baseSequence: 0 lastSequence: 0 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 90 magic: 2 compressType: NONE position: 180 sequence: 0 headerKeys: [] key: k3 payload: idempotent-message-3
         */
        // producer.transactionManager: { currentState: , producerIdAndEpoch: { producerId=10, epoch=0 }}
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k4", "idempotent-message-4"));
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k5", "idempotent-message-5"));
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k6", "idempotent-message-6"));
        nap();
        producer.flush();
        /* partition log entry
baseOffset: 3 lastOffset: 5 count: 3 baseSequence: 1 lastSequence: 3 producerId: 2006 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 270 CreateTime: 1663408067252 size: 148 magic: 2 compresscodec: NONE crc: 1314807965 isvalid: true
| offset: 3 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408067252 baseOffset: 3 lastOffset: 5 baseSequence: 1 lastSequence: 3 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 148 magic: 2 compressType: NONE position: 270 sequence: 1 headerKeys: [] key: k4 payload: idempotent-message-4
| offset: 4 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408067252 baseOffset: 3 lastOffset: 5 baseSequence: 1 lastSequence: 3 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 148 magic: 2 compressType: NONE position: 270 sequence: 2 headerKeys: [] key: k5 payload: idempotent-message-5
| offset: 5 isValid: true crc: null keySize: 2 valueSize: 20 CreateTime: 1663408067252 baseOffset: 3 lastOffset: 5 baseSequence: 1 lastSequence: 3 producerEpoch: 0 partitionLeaderEpoch: 0 batchSize: 148 magic: 2 compressType: NONE position: 270 sequence: 3 headerKeys: [] key: k6 payload: idempotent-message-6
         */
        producer.close();

        // the producerId keeps increasing even if we re-run the main() function
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(LINGER_MS_CONFIG, 500);
        producerProps.put(CLIENT_ID_CONFIG, "idempotent-client");

        // When security enabled, a IdempotentWrite permission is required on 6.x, but not 7.x platform
        producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. Nothing observed from __transaction_state topic.
  2. Consumers with read_uncommitted and read_committed see the messages at the same time
  3. transactionManager (in client side) is involved, producerId is associated with a producer
  4. a transactionCoordinator (on broker side) is NOT involved
  5. Partition Log Entry
    - includes: producerId, producerEpoch, baseSequence=lastSequence=sequence
    - a Batch Log Entry includes extra: baseSequence, lastSequence,
      - each message in batch includes its unique sequence
  6. For the same producerId+producerEpoch combination, sequence keeps increasing
 */
