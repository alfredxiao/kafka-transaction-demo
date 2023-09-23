package xiaoyf.demo.kafka.transaction.transactional.streamlike;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static xiaoyf.demo.kafka.transaction.helper.Constants.BOOTSTRAP_SERVERS;

/**
 * WindowedAggregationDemo demonstrates how a tumbling windowing works in a Kafka Streaming
 * application which utilises streaming time as basis of windowing.
 */
@Slf4j
public class WindowedAggregationDemo {
    final static String GROUP_ID = "windowed-aggregation";
    final static Duration WINDOW_SIZE = Duration.ofSeconds(10);
    final static Duration GRACE = Duration.ofSeconds(30);

    KafkaProducer<String, String> producer = createProducer();
    KafkaConsumer<String, String> consumer = createConsumer();

    TreeMap<Window, Aggregated> aggregatedWindows;

    long streamTime = -1L;
    public static void main(String[] args) throws Exception {
        WindowedAggregationDemo demo = new WindowedAggregationDemo();
        demo.demoRun();
    }

    void init() {

        producer = createProducer();
        consumer = createConsumer();

        consumer.subscribe(singleton("w1"));

        aggregatedWindows = new TreeMap<>();

        producer.initTransactions();
    }

    void demoRun() {

        init();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // todo:
            //  increase stream time and then add to windows
            //  keep a recently closed windows set, log warn when too late message comes
            //  allow different instance of the processor to be created with diff consumer group id, grace, and interval
            records.forEach(this::processRecord);

            // todo:
            // save closed window set in a topic, and loads it at beginning of processing
            // make this closed window state part of transaction
            // also close windows as a group rather than individual (otherwise, one window closes successfully,
            //   another one with diff key but the same window start/end fails, but commit signals all input
            //   are consumed hence those failed will be missed)
            // close window at the end
        }

    }

    void processRecord(ConsumerRecord<String, String> record) {
        if (record.timestamp() > streamTime) {
            streamTime = record.timestamp();
            maybeCloseWindows();
        }

        aggregateRecord(record);
    }

    private void maybeCloseWindows() {
        Iterator<Map.Entry<Window, Aggregated>> it = aggregatedWindows.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Window, Aggregated> entry = it.next();
            if (entry.getKey().canBeClosed(streamTime)) {
                closeWindow(entry.getKey());
                it.remove();
            }
        }
    }

    void closeWindow(Window window) {
        Aggregated aggregated = aggregatedWindows.get(window);
        List<ConsumerRecord<String,String>> records = aggregated.getRecords();

        if (records.size() > 0) {
            String aggregatedKeys =
                    records.stream().map(ConsumerRecord::key).collect(Collectors.joining(","));
            String aggregatedValues = String.format(
                    "[%s, %s):{%s}",
                    format(window.getStart()),
                    records.stream().map(ConsumerRecord::value).collect(Collectors.joining(",")),
                    format(window.getEnd())
            );

            producer.beginTransaction();
            producer.send(new ProducerRecord<>("w2", aggregatedKeys, aggregatedValues));

            // to fix: only offset up to oldWin
            producer.sendOffsetsToTransaction(aggregated.getOffsets(), GROUP_ID);

            log.info("Commit Tx: {}, {}",
                    aggregatedKeys,
                    aggregatedValues);

            producer.commitTransaction();
        }
    }

    void aggregateRecord(ConsumerRecord<String, String> record) {
        Window window = windowFor(record.timestamp(), WINDOW_SIZE.toMillis());

        // tofix: check window previously closed/expired, then ignore record

        aggregatedWindows.putIfAbsent(window, new Aggregated(new ArrayList<>(), new HashMap<>()));

        aggregatedWindows.get(window).getRecords().add(record);
        aggregatedWindows.get(window).getOffsets().put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
    }

    @Getter
    @AllArgsConstructor
    static class Aggregated {
        List<ConsumerRecord<String,String>> records;
        Map<TopicPartition, OffsetAndMetadata> offsets;
    }

    @Getter
    @AllArgsConstructor
    static class Window implements Comparable<Window> {
        long start;
        long end;

        boolean canBeClosed(long streamTime) {
            return end + GRACE.toMillis() <= streamTime ;
        }

        @Override
        public int compareTo(Window o) {
            if (this.start - o.start > 0) {
                return 1;
            }

            if (this.start - o.start == 0) {
                return 0;
            }

            return -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Window window = (Window) o;

            if (start != window.start) return false;
            return end == window.end;
        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }

    static Window windowFor(long now, long windowSizeMillis) {
        final long remainMs = now % windowSizeMillis;
        if (remainMs == 0) {
            return new Window(now, now + windowSizeMillis);
        }

        return new Window(now - remainMs, now - remainMs + windowSizeMillis);
    }

    public static String format(final Long epoch) {
        if (epoch == null) {
            return "";
        }

        return Instant.ofEpochMilli(epoch).toString();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(TRANSACTIONAL_ID_CONFIG, "exactly-once-aggregation");
        producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(producerProps);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        producerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        producerProps.put(GROUP_ID_CONFIG, GROUP_ID);
        producerProps.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        producerProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(producerProps);
    }

}