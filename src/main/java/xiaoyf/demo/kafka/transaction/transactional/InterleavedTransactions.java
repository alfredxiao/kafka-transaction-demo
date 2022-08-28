package xiaoyf.demo.kafka.transaction.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.helper.Constants.TRANSACTION_DEMO_TOPIC;

@Slf4j
public class InterleavedTransactions {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> txProducer1 = createTransactionalProducer("tx1");
        KafkaProducer<String, String> txProducer2 = createTransactionalProducer("tx2");
        KafkaProducer<String, String> nonTxProducer = createNonTransactionalProducer();

        txProducer1.initTransactions();
        txProducer1.beginTransaction();
        txProducer1.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "t1", "t1")).get();
                nonTxProducer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "n1", "n1")).get();
            txProducer2.initTransactions();
            txProducer2.beginTransaction();
            txProducer2.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "t2", "t2")).get();
                nonTxProducer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "n2", "n2")).get();
            txProducer2.commitTransaction();
                nonTxProducer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "n3", "n3")).get();
        txProducer1.commitTransaction();
    }

    private static KafkaProducer<String, String> createTransactionalProducer(final String transactionalId) {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put("transactional.id", transactionalId);

        return new KafkaProducer<>(producerProps);
    }

    private static KafkaProducer<String, String> createNonTransactionalProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. Consumer read_committed only read committed records - which means uncommitted transactions keeps all records
     written after from being consumed, no matter they are written by non-transactional producer or another
     transactional producer
 */