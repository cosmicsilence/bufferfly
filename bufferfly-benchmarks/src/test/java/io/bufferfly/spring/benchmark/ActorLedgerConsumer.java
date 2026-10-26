package io.bufferfly.spring.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The <b>BufferFly actor</b> approach: the same 3 topics are consumed, but each
 * (topic, partition) is owned by a single {@link PgLedgerActor}. The actor keeps
 * the running balance in memory (single-writer, no locks) and streams changes to
 * Postgres through the natural-batching persistence runner, which coalesces many
 * messages into one bulk transaction.
 *
 * <p>Kafka records are polled on a thread per topic and routed to the owning
 * actor via its {@link ActorReference#tell}. The actor's mailbox absorbs the
 * burst; the {@code BatchingPersistenceRunner} drains it into fat transactions.
 * One commit covers up to {@code batchSize} messages, so WAL/commit pressure on
 * Postgres is a small fraction of the classic per-message path.
 */
public final class ActorLedgerConsumer implements AutoCloseable {

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final PgActorLedgerOperations operations;
    private final int batchSize;
    private final long timeoutMs;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ActorManager manager = new ActorManager();
    private final Map<String, ActorReference<BenchmarkRecord>> actors = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicBoolean running =
            new java.util.concurrent.atomic.AtomicBoolean(true);
    private final List<Thread> pollThreads = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<KafkaConsumer<String, String>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();

    public ActorLedgerConsumer(String bootstrapServers, List<String> topics, String groupId,
                               PgActorLedgerOperations operations, int batchSize, long timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.topics = List.copyOf(topics);
        this.groupId = groupId;
        this.operations = operations;
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Starts one poll thread per topic, routing records to a per-partition
     * actor, and waits until {@code store} reports {@code expectedTotal} changes
     * persisted.
     *
     * @param store         the shared store, polled for the persisted count
     * @param expectedTotal total changes to wait for
     * @return milliseconds from start to all persisted
     */
    public long runUntil(PgLedgerStore store, long expectedTotal) throws InterruptedException {
        long start = System.nanoTime();

        for (String topic : topics) {
            KafkaConsumer<String, String> consumer = newConsumer();
            consumer.subscribe(List.of(topic));
            consumers.add(consumer);
            Thread poll = new Thread(() -> pollLoop(consumer), "actor-poll-" + topic);
            poll.start();
            pollThreads.add(poll);
        }

        // wait until all changes are durably persisted
        while (store.totalChanges() < expectedTotal) {
            Thread.sleep(25);
        }
        long elapsed = (System.nanoTime() - start) / 1_000_000L;
        running.set(false);
        return elapsed;
    }

    private void pollLoop(KafkaConsumer<String, String> consumer) {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    route(record);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            // expected on close()
        } finally {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception ignored) {
            }
        }
    }

    private void route(ConsumerRecord<String, String> record) {
        try {
            AddToAccount cmd = mapper.readValue(record.value(), AddToAccount.class);
            String key = record.topic() + "-" + record.partition();
            ActorReference<BenchmarkRecord> actor = actors.computeIfAbsent(key, k -> {
                PersistenceConfig<PgLedgerChange> config = PersistenceConfig.builder(operations)
                        .batchSize(batchSize)
                        .timeoutMs(timeoutMs)
                        .maxRetries(3)
                        .build();
                return manager.start(new PgLedgerActor("pg-ledger-" + key, config));
            });
            actor.tell(new BenchmarkRecord(record.topic(), record.partition(), record.offset(), cmd));
        } catch (Exception e) {
            throw new RuntimeException("actor router failed", e);
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        Map<String, Object> props = new java.util.HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    @Override
    public void close() {
        running.set(false);
        consumers.forEach(c -> {
            try {
                c.wakeup();
            } catch (Exception ignored) {
            }
        });
        pollThreads.forEach(t -> {
            try {
                t.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        actors.values().forEach(ActorReference::stop);
        manager.shutdown();
    }
}
