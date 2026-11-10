package io.bufferfly.spring.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
     * topic so the approaches' tables are directly comparable.
     *
     * @return total messages produced across all topics
     */
    public long produce(List<String> topics, int messagesPerTopic, int accountsPerTopic) {
        return produce(topics, messagesPerTopic, accountsPerTopic, false);
    }

    /**
     * Like {@link #produce(List, int, int)}, but optionally shuffles the order of
     * each topic's messages (fixed seed, so runs are reproducible). Each account
     * still receives exactly {@code messagesPerTopic / accountsPerTopic} credits;
     * only their order changes, so the same account can appear several times
     * within one poll.
     *
     * @return total messages produced across all topics
     */
    public long produce(List<String> topics, int messagesPerTopic, int accountsPerTopic, boolean shuffled) {
        long total = 0;
        Random random = new Random(SHUFFLE_SEED);
        try {
            for (String topic : topics) {
                List<Integer> accountOrder = new ArrayList<>(messagesPerTopic);
                for (int i = 0; i < messagesPerTopic; i++) {
                    accountOrder.add(i % accountsPerTopic);
                }
                if (shuffled) {
                    Collections.shuffle(accountOrder, random);
                }
                for (int accountIndex : accountOrder) {
                    String account = topic + "-ACC-" + accountIndex;
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

    private static final long SHUFFLE_SEED = 42L;

    @Override
    public void close() {
        producer.close();
    }
}
