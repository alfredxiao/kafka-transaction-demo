package xiaoyf.demo.kafka.transaction;

public interface Constants {
    String BOOTSTRAP_SERVERS = "localhost:9092";
    String GROUPD_ID_1 = "group1";
    String LOWERCASE_TOPIC = "lowercase";
    String UPPERCASE_TOPIC = "uppercase";
    String SINGLE_TRANSACTIONAL_PRODUCER_TOPIC = "single-transactional-producer";
    String ANOTHER_TOPIC = "another";
    String ONE_TX_AND_ONE_NON_TX_PRODUCER_TOPIC = "one-tx-and-one-non-tx-producer";
}
