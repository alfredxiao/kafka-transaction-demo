package xiaoyf.demo.kafka.transaction.nontransactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.Constants.SINGLE_TRANSACTIONAL_PRODUCER_TOPIC;

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
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v1")).get();
        /*  producer: {
              clientId: producer-1,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
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
                producerIdAndEpoch: {
                  producerId: -1,
                  epoch: -1
                }
              }
           } */
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v2")).get();
        /*  producer: {
              clientId: producer-2,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
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
