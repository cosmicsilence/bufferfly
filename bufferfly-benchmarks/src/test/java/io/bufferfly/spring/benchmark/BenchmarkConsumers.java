package io.bufferfly.spring.benchmark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * The one Kafka consumer configuration shared by every benchmark approach, so
 * they all fetch the same records per poll and differ only in what they do with
 * them.
 */
public final class BenchmarkConsumers {

    /**
     * Records per {@code poll()} for every approach. The actor's natural batch
     * size is set to the same value, so one actor batch and one batch-consumer
     * transaction cover the same number of records.
     */
    public static final int MAX_POLL_RECORDS = 500;

    private BenchmarkConsumers() {
    }

    public static KafkaConsumer<String, String> newConsumer(String bootstrapServers, String groupId, boolean autoCommit) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
