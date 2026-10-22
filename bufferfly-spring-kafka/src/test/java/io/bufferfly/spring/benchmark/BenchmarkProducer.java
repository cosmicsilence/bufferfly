package io.bufferfly.spring.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

/**
 * Produces the benchmark workload: {@code messagesPerTopic} credits spread
 * across {@code accountsPerTopic} accounts, to each of the given topics. The
 * account number is the message key, so all events for an account land on the
 * same partition and are processed in order (per-partition single writer on the
 * actor side; per-account lock on the classic side).
 *
 * <p>The amount is always {@code +1} so the expected final per-account balance
 * is simply the number of events for that account — trivial to assert exactly,
 * which is what proves neither approach lost or double-counted a message.
 */
public final class BenchmarkProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public BenchmarkProducer(String bootstrapServers) {
        Map<String, Object> props = new java.util.HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Publishes {@code messagesPerTopic} to each topic, round-robining across
     * {@code accountsPerTopic} per-topic accounts. Account keys are namespaced by
     * topic so the two approaches' tables are directly comparable.
     *
     * @return total messages produced across all topics
     */
    public long produce(List<String> topics, int messagesPerTopic, int accountsPerTopic) {
        long total = 0;
        try {
            for (String topic : topics) {
                for (int i = 0; i < messagesPerTopic; i++) {
                    String account = topic + "-ACC-" + (i % accountsPerTopic);
                    String json = mapper.writeValueAsString(new AddToAccount(account, 1L, "credit"));
                    producer.send(new ProducerRecord<>(topic, account, json));
                    total++;
                }
            }
            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException("produce failed", e);
        }
        return total;
    }

    @Override
    public void close() {
        producer.close();
    }
}
