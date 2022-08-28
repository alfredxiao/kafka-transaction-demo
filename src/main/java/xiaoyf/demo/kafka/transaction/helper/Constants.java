package xiaoyf.demo.kafka.transaction.helper;

public interface Constants {
    String BOOTSTRAP_SERVERS = "localhost:9092";
    String GROUPD_ID_1 = "group1";
    String LOWERCASE_TOPIC = "lowercase";
    String UPPERCASE_TOPIC = "uppercase";
    String TRANSACTION_DEMO_TOPIC = "transaction-demo";
    String ONE_TX_AND_ONE_NON_TX_PRODUCER_TOPIC = "one-tx-and-one-non-tx-producer";
}
