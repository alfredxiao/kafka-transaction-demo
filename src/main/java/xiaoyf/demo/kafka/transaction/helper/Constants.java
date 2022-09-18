package xiaoyf.demo.kafka.transaction.helper;

public interface Constants {
    String BOOTSTRAP_SERVERS = "localhost:9092";

    String NON_IDEMPOTENT_TOPIC = "non-idempotent-demo";
    String IDEMPOTENT_TOPIC = "idempotent-demo";
    String TRANSACTION_DEMO_TOPIC = "transaction-demo";
    String OFFSET_DEMO_INPUT_TOPIC = "offset-demo-input";
    String OFFSET_DEMO_OUTPUT_TOPIC = "offset-demo-output";
}
