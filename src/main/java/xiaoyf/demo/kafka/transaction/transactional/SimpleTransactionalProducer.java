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
 * SimpleTransactionalProducer demonstrates how transaction state changes from the producer's perspective.
 */
@Slf4j
public class SimpleTransactionalProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        /*  producer: {
              clientId: producer-simple-tx,
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
              clientId: producer-simple-tx,
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
              clientId: producer-simple-tx,
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
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "transactional-message-1")).get();
        /*  producer: {
              clientId: producer-simple-tx,
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
              clientId: producer-simple-tx,
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
              clientId: producer-simple-tx,
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
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k2", "transactional-message-2")).get();
        /*  producer: {
              clientId: producer-simple-tx,
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
              clientId: producer-simple-tx,
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

        producer = createProducer();
        producer.initTransactions();
        // producer.transactionManager.producerIdAndEpoch: { producerId: 2, epoch: 28 }
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k3", "transactional-message-3")).get();
        producer.commitTransaction();
        producer.close();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        // implies idempotence mode? -> YES, 'enable.idempotence' set to 'true' automatically,
        //                              and it CANNOT be set to 'false' actually
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "simple-tx");

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. beginTransaction does not really start a transaction, it only marks a transaction is ready to start; send()
     actually starts a transaction - start of a transaction is of importance as it relates to transaction timeout
     calculation, by default 60 seconds after the start
  2. The same producer (same transactional.id) gets a bumped epoch when it reconnects
 */