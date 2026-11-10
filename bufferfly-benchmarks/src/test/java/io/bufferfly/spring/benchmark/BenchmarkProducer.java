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
 * Produces the benchmark workload: {@code messages} credits spread across
 * {@code accounts} accounts, to one topic. The account number is the message
 * key, so Kafka's default partitioner decides each account's partition; all
 * events for an account land on the same partition and are processed in order.
 *
 * <p>The amount is always {@code +1} so the expected final per-account balance
 * is simply the number of events for that account — trivial to assert exactly,
 * which is what proves no approach lost or double-counted a message.
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
     * Publishes {@code messages} credits to {@code topic}, round-robin over
     * {@code accounts} accounts ({@code ACC-0}, {@code ACC-1}, ...) or, if
     * {@code shuffled}, in a random order (fixed seed, so runs are
     * reproducible). Each account receives exactly {@code messages / accounts}
     * credits either way; only their order changes.
     *
     * @return messages produced
     */
    public long produce(String topic, int messages, int accounts, boolean shuffled) {
        List<Integer> accountOrder = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            accountOrder.add(i % accounts);
        }
        if (shuffled) {
            Collections.shuffle(accountOrder, new Random(SHUFFLE_SEED));
        }
        long total = 0;
        try {
            for (int accountIndex : accountOrder) {
                String account = "ACC-" + accountIndex;
                String json = mapper.writeValueAsString(new AddToAccount(account, 1L, "credit"));
                producer.send(new ProducerRecord<>(topic, account, json));
                total++;
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
