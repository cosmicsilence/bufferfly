package io.bufferfly.spring.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The <b>classic</b> approach: the pipeline BufferFly is designed to replace.
 *
 * <p>For each of the {@code topics}, one {@link KafkaConsumer} polls records and
 * dispatches them to a fixed thread pool of {@link #THREADS_PER_TOPIC} workers.
 * Each worker computes the account's new balance <b>under a per-account
 * {@link ReentrantLock}</b> (the lock-based substitute for the actor's
 * single-writer discipline) and persists the change in a <b>single tiny
 * transaction per message</b> via {@link PgLedgerStore#applyOneLocked} —
 * BEGIN; INSERT; UPDATE; COMMIT, one WAL flush and one commit per message.
 *
 * <p>This is deliberately the naive, contention-heavy design:
 * <ul>
 *   <li><b>WAL/commit contention</b> — thousands of micro-transactions hammer
 *       Postgres's WAL; {@code pg_stat_database.xact_commit} climbs by roughly
 *       one per message and {@code pg_stat_wal} shows the write amplification.</li>
 *   <li><b>Lock contention</b> — all workers serialize on the per-account lock
 *       for hot accounts, so added threads do not add throughput on skewed keys.</li>
 * </ul>
 * Correctness still holds (the per-account lock + idempotent insert keep the sum
 * right), which is the point: same result, far more DB work.
 */
public final class ClassicLockingConsumer implements AutoCloseable {

    public static final int THREADS_PER_TOPIC = 7;

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final PgLedgerStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    /** One lock per account — serializes the read-modify-write of each balance. */
    private final Map<String, Lock> accountLocks = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong processed = new AtomicLong();
    private final List<Thread> pollThreads = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<ExecutorService> pools = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<KafkaConsumer<String, String>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();

    public ClassicLockingConsumer(String bootstrapServers, List<String> topics, String groupId, PgLedgerStore store) {
        this.bootstrapServers = bootstrapServers;
        this.topics = List.copyOf(topics);
        this.groupId = groupId;
        this.store = store;
    }

    /**
     * Starts one poll thread + 7-thread worker pool per topic and processes
     * until {@code expectedTotal} messages have been persisted.
     *
     * @param expectedTotal messages to wait for across all topics
     * @return milliseconds from start to all persisted
     */
    public long runUntil(long expectedTotal) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        long start = System.nanoTime();

        for (String topic : topics) {
            ExecutorService pool = Executors.newFixedThreadPool(THREADS_PER_TOPIC);
            pools.add(pool);
            KafkaConsumer<String, String> consumer = newConsumer();
            consumer.subscribe(List.of(topic));
            consumers.add(consumer);

            Thread poll = new Thread(() -> pollLoop(consumer, pool, expectedTotal, done), "classic-poll-" + topic);
            poll.start();
            pollThreads.add(poll);
        }

        done.await();
        long elapsed = (System.nanoTime() - start) / 1_000_000L;
        running.set(false);
        return elapsed;
    }

    private void pollLoop(KafkaConsumer<String, String> consumer,
                          ExecutorService pool,
                          long expectedTotal,
                          CountDownLatch done) {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    pool.execute(() -> handle(record, expectedTotal, done));
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

    private void handle(ConsumerRecord<String, String> record, long expectedTotal, CountDownLatch done) {
        try {
            AddToAccount cmd = mapper.readValue(record.value(), AddToAccount.class);
            Lock lock = accountLocks.computeIfAbsent(cmd.accountNumber(), k -> new ReentrantLock());
            lock.lock();
            try {
                // one tiny transaction per message — the contention-heavy path
                store.applyOneLocked(record.topic(), record.partition(), record.offset(),
                        cmd.accountNumber(), cmd.amount());
            } finally {
                lock.unlock();
            }
            long n = processed.incrementAndGet();
            if (n >= expectedTotal) {
                done.countDown();
            }
        } catch (Exception e) {
            throw new RuntimeException("classic handler failed", e);
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

    public long processed() {
        return processed.get();
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
        pools.forEach(ExecutorService::shutdownNow);
    }
}
