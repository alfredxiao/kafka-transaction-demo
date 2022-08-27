package xiaoyf.demo.kafka.transaction.nontransactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.kafka.transaction.Constants.SINGLE_TRANSACTIONAL_PRODUCER_TOPIC;

@Slf4j
public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        /*  producer: {
              clientId: producer-1,
              transactionManager: null
           } */
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v1")).get();
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
        producer.send(new ProducerRecord<>(SINGLE_TRANSACTIONAL_PRODUCER_TOPIC, "k1", "v1")).get();
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
