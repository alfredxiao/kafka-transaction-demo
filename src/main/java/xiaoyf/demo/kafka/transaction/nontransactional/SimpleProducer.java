package xiaoyf.demo.kafka.transaction.nontransactional;

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
public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        /*  producer: {
              clientId: producer-1,
              transactionManager: null
           } */
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k1", "simple-message-1")).get();
        /*  producer: {
              clientId: producer-1,
              transactionManager: null
           } */
        producer.close();
        producer = createProducer();
        /*  producer: {
              clientId: producer-2,
              transactionManager: null
           } */
        producer.send(new ProducerRecord<>(TRANSACTION_DEMO_TOPIC, "k2", "simple-message-2")).get();
        /*  producer: {
              clientId: producer-2,
              transactionManager: null
           } */
        producer.close();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        return new KafkaProducer<>(producerProps);
    }
}

/* NOTE
  1. No transactionManager, no producerId are involved
 */
