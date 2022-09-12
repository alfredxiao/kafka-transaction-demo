package xiaoyf.demo.kafka.transaction.helper;

public interface Constants {
    String BOOTSTRAP_SERVERS = "localhost:9092";

    String NON_IDEMPOTENT_TOPIC = "non-idempotent-demo";
    String IDEMPOTENT_TOPIC = "idempotent-demo";
    String IDEMPOTENT_BATCH_TOPIC = "idempotent-batch-demo";
    String TRANSACTION_DEMO_TOPIC = "transaction-demo";
    String ONE_TX_AND_ONE_NON_TX_PRODUCER_TOPIC = "one-tx-and-one-non-tx-producer";
}
