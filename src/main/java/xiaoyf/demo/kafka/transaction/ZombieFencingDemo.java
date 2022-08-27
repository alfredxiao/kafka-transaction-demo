package xiaoyf.demo.kafka.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.Constants.BOOTSTRAP_SERVERS;

@Slf4j
public class ZombieFencingDemo {

    final static String TOPIC = Constants.ONE_TX_AND_ONE_NON_TX_PRODUCER_TOPIC;
    final static String TRANSACTIONAL_ID = "fencing1";

    public static void main(String[] args) throws Exception {
        fencedOffWhenSendingToo();
    }

    public static void fencedOffWhenCommitting(String[] args) throws Exception {
        try (KafkaProducer<String, String> txProducer1 = createTransactionalProducer(TRANSACTIONAL_ID);
             KafkaProducer<String, String> txProducer2 = createTransactionalProducer(TRANSACTIONAL_ID)) {

            txProducer1.initTransactions(); // -> triggers a state=Empty, with producerId, producerEpoch
            txProducer1.beginTransaction();
            txProducer1.send(new ProducerRecord<>(TOPIC, "t1", "t1")).get();  // starts tx, state=Ongoing

                txProducer2.initTransactions(); // receives same producerId, but producerEpoch+1, then triggers
                                                // a state=PrepareAbort and a state=CompleteAbort
                                                // ALSO, receives same producerId, but producerEpoch+2, then triggers
                                                // a state=Empty
                txProducer2.beginTransaction();
                txProducer2.send(new ProducerRecord<>(TOPIC, "t2", "t2")).get(); // triggers state=Ongoing
                txProducer2.commitTransaction(); // triggers state=PrepareCommit & state=CompleteCommit

            txProducer1.commitTransaction(); // receives a ProducerFencedException
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fencedOffWhenSending() throws Exception {
        try (KafkaProducer<String, String> txProducer1 = createTransactionalProducer(TRANSACTIONAL_ID);
             KafkaProducer<String, String> txProducer2 = createTransactionalProducer(TRANSACTIONAL_ID)) {

            txProducer1.initTransactions(); // -> triggers a state=Empty, with producerId, producerEpoch
            txProducer1.beginTransaction();

                txProducer2.initTransactions(); // receives same producerId, but producerEpoch+1, then triggers
                                                // a state=Empty
                txProducer2.beginTransaction();
                txProducer2.send(new ProducerRecord<>(TOPIC, "t2", "t2")).get(); // triggers state=Ongoing
                txProducer2.commitTransaction(); // triggers state=PrepareCommit & state=CompleteCommit

            txProducer1.send(new ProducerRecord<>(TOPIC, "t1", "t1")).get();  // receives a ProducerFencedException
            txProducer1.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fencedOffWhenSendingToo() throws Exception {
        try (KafkaProducer<String, String> txProducer1 = createTransactionalProducer(TRANSACTIONAL_ID);
             KafkaProducer<String, String> txProducer2 = createTransactionalProducer(TRANSACTIONAL_ID)) {

            txProducer1.initTransactions(); // -> triggers a state=Empty, with producerId, producerEpoch

                txProducer2.initTransactions(); // receives same producerId, but producerEpoch+1, then triggers
                                                // a state=Empty
                txProducer2.beginTransaction();
                txProducer2.send(new ProducerRecord<>(TOPIC, "t2", "t2")).get(); // triggers state=Ongoing
                txProducer2.commitTransaction(); // triggers state=PrepareCommit & state=CompleteCommit

            txProducer1.beginTransaction();
            txProducer1.send(new ProducerRecord<>(TOPIC, "t1", "t1")).get();  // receives a ProducerFencedException
            txProducer1.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaProducer<String, String> createTransactionalProducer(final String transactionalId) {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        return new KafkaProducer<>(producerProps);
    }
}
