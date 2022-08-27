package xiaoyf.demo.kafka.transaction.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
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

/**
 * SimpleTransactionalProducer demonstrates
 */
@Slf4j
public class SimpleTransactionalProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: UNINITIALIZED,
                transactionStarted: false,
                transactionCoordinator: null,
                producerIdAndEpoch: {
                  producerId: -1,
                  epoch: -1
                }
              }
           } */
        producer.initTransactions();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.beginTransaction();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: IN_TRANSACTION,
                transactionStarted: false,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v1")).get();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: IN_TRANSACTION,
                transactionStarted: true,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.commitTransaction();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.beginTransaction();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: IN_TRANSACTION,
                transactionStarted: false,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k2", "v2")).get();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: IN_TRANSACTION,
                transactionStarted: true,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.commitTransaction();
        /*  producer: {
              clientId: producer-simple-transactional-producer,
              transactionManager: {
                currentState: READY,
                transactionStarted: false,
                transactionCoordinator: 192-168-1-9.tpgi.com.au:9092 (id: 0 rack: null)
                producerIdAndEpoch: {
                  producerId: 2,
                  epoch: 27
                }
              }
           } */
        producer.close();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        // implies idempotence mode?
        producerProps.put("transactional.id", "simple-transactional-producer");

        return new KafkaProducer<>(producerProps);
    }

}
