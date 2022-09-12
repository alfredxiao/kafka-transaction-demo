package xiaoyf.demo.kafka.transaction.nontransactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.IDEMPOTENT_TOPIC;

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
        /*  producer: {
              clientId: producer-1,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId=9, epoch=0
                }
              }
           } */
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
        producer.send(new ProducerRecord<>(IDEMPOTENT_TOPIC, "k2", "idempotent-message-2")).get();
        /*  producer: {
              clientId: producer-2,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId: 10,
                  epoch: 0
                }
              }
           } */
        // producer.transactionManager: { currentState: , producerIdAndEpoch: { producerId=10, epoch=0 }}
        producer.close();

        // the producerId keeps increasing even if we re-run the main() function
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        // When security enabled, a IdempotentWrite permission is required on 6.x, but not 7.x platform
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. Nothing observed from __transaction_state topic.
  2. Consumers with read_uncommitted and read_committed see the messages at the same time
  3. transactionManager (in client side) is involved, producerId is associated with a producer
  4. a transactionCoordinator (on broker side) is NOT involved
 */
