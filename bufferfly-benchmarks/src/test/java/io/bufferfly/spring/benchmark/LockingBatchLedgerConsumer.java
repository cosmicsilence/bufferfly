package io.bufferfly.spring.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A <b>batching, lock-based</b> Kafka consumer — the strongest conventional
 * alternative to the actor, built from standard concurrency building blocks
 * instead of BufferFly.
 *
 * <p>Like the actor, it keeps balances in memory and writes to Postgres in
 * bulk. Unlike the actor, it does so in lock-step with Kafka. For each topic,
 * one poll thread runs this loop:
 * <ol>
 *   <li>{@code poll()} up to {@code max.poll.records} (500) records;</li>
 *   <li>hand every record to a pool of {@link #THREADS_PER_TOPIC} workers, each
 *       updating its account's balance in a shared in-memory map <b>under a
 *       per-account {@link ReentrantLock}</b>, and wait for all of them;</li>
 *   <li>write the whole poll <b>synchronously in one transaction</b>
 *       ({@link PgLedgerStore#applyPollBatch}: bulk change insert, bulk balance
 *       upsert, offset upsert);</li>
 *   <li>{@code commitSync} the Kafka offsets, then poll again.</li>
 * </ol>
 * The database write blocks the next poll, so fetching, processing, and writing
 * never overlap — the main structural difference from the actor, whose mailbox
 * and persistence runner let the three run concurrently.
 *
 * <p>Records for the same account within one poll are handed to different
 * workers, so they contend on that account's lock and may be applied in a
 * different order than their Kafka offsets. The final balance is still correct
 * (every update is serialized by the lock), but the per-change
 * {@code resulting_balance} can be out of order — see
 * {@link PgLedgerStore#outOfOrderChanges()}.
 */
public final class LockingBatchLedgerConsumer implements AutoCloseable {

    public static final int THREADS_PER_TOPIC = 7;

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final PgLedgerStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    /** One lock per account — serializes the read-modify-write of each balance. */
    private final Map<String, Lock> accountLocks = new ConcurrentHashMap<>();
    /** In-memory running balance per account, guarded by that account's lock. */
    private final Map<String, Long> balances = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong persisted = new AtomicLong();
    /** Lock acquisitions that found the account lock already held and had to wait. */
    private final AtomicLong contendedAcquisitions = new AtomicLong();
    /** Time spent per phase, summed over all poll threads. */
    private final AtomicLong processNanos = new AtomicLong();
    private final AtomicLong dbNanos = new AtomicLong();
    private final AtomicLong kafkaCommitNanos = new AtomicLong();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<Thread> pollThreads = new CopyOnWriteArrayList<>();
    private final List<ExecutorService> pools = new CopyOnWriteArrayList<>();
    private final List<KafkaConsumer<String, String>> consumers = new CopyOnWriteArrayList<>();

    public LockingBatchLedgerConsumer(String bootstrapServers, List<String> topics, String groupId,
                                      PgLedgerStore store) {
        this.bootstrapServers = bootstrapServers;
        this.topics = List.copyOf(topics);
        this.groupId = groupId;
        this.store = store;
    }

    /**
     * Starts one poll thread + worker pool per topic and processes until
     * {@code expectedTotal} changes have been committed to the database.
     *
     * @param expectedTotal changes to wait for across all topics
     * @return milliseconds from start to all persisted
     */
    public long runUntil(long expectedTotal) throws InterruptedException {
        long start = System.nanoTime();

        for (String topic : topics) {
            ExecutorService pool = Executors.newFixedThreadPool(THREADS_PER_TOPIC);
            pools.add(pool);
            KafkaConsumer<String, String> consumer = newConsumer();
            consumer.subscribe(List.of(topic));
            consumers.add(consumer);
            Thread poll = new Thread(() -> pollLoop(consumer, pool), "lockbatch-poll-" + topic);
            poll.start();
            pollThreads.add(poll);
        }

        while (persisted.get() < expectedTotal) {
            Throwable error = failure.get();
            if (error != null) {
                throw new IllegalStateException("locking batch consumer failed", error);
            }
            Thread.sleep(5);
        }
        long elapsed = (System.nanoTime() - start) / 1_000_000L;
        running.set(false);
        return elapsed;
    }

    private void pollLoop(KafkaConsumer<String, String> consumer, ExecutorService pool) {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    processPoll(consumer, pool, records);
                }
            }
        } catch (WakeupException ignored) {
            // expected on close()
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        } finally {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception ignored) {
            }
        }
    }

    private void processPoll(KafkaConsumer<String, String> consumer, ExecutorService pool,
                             ConsumerRecords<String, String> records)
            throws InterruptedException, ExecutionException {
        long t0 = System.nanoTime();
        // 1) process every record of the poll on the worker pool and wait for all
        List<Callable<PgLedgerChange>> tasks = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            tasks.add(() -> applyLocked(record));
        }
        List<PgLedgerChange> changes = new ArrayList<>(tasks.size());
        for (Future<PgLedgerChange> f : pool.invokeAll(tasks)) {
            changes.add(f.get());
        }

        // 2) the balances of the accounts this poll touched are now final for the
        //    poll (accounts are namespaced per topic, so only this thread's
        //    workers update them, and they have all finished)
        Map<String, Long> finalBalances = new HashMap<>();
        changes.forEach(c -> finalBalances.put(c.accountNumber(), balances.get(c.accountNumber())));
        long t1 = System.nanoTime();

        // 3) one synchronous SQL batch for the whole poll, before the next poll
        store.applyPollBatch(changes, finalBalances);
        long t2 = System.nanoTime();

        // 4) commit the Kafka offsets only after the database committed
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        for (TopicPartition tp : records.partitions()) {
            offsets.put(tp, new OffsetAndMetadata(records.records(tp).getLast().offset() + 1));
        }
        consumer.commitSync(offsets);
        long t3 = System.nanoTime();

        processNanos.addAndGet(t1 - t0);
        dbNanos.addAndGet(t2 - t1);
        kafkaCommitNanos.addAndGet(t3 - t2);
        persisted.addAndGet(changes.size());
    }

    /** Applies one record to its account's in-memory balance under the account lock. */
    private PgLedgerChange applyLocked(ConsumerRecord<String, String> record) throws Exception {
        AddToAccount cmd = mapper.readValue(record.value(), AddToAccount.class);
        Lock lock = accountLocks.computeIfAbsent(cmd.accountNumber(), k -> new ReentrantLock());
        if (!lock.tryLock()) {
            // another worker holds this account right now: count it, then wait
            contendedAcquisitions.incrementAndGet();
            lock.lock();
        }
        try {
            long balance = balances.getOrDefault(cmd.accountNumber(), 0L) + cmd.amount();
            balances.put(cmd.accountNumber(), balance);
            return new PgLedgerChange(record.topic(), record.partition(), record.offset(),
                    cmd.accountNumber(), cmd.amount(), balance);
        } finally {
            lock.unlock();
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        return BenchmarkConsumers.newConsumer(bootstrapServers, groupId, false);
    }

    /** @return lock acquisitions that had to wait for another worker. */
    public long contendedLockAcquisitions() {
        return contendedAcquisitions.get();
    }

    /** @return ms spent processing records (parse + locked balance update), summed over poll threads. */
    public long processMillis() {
        return processNanos.get() / 1_000_000L;
    }

    /** @return ms spent in the synchronous database transaction, summed over poll threads. */
    public long dbMillis() {
        return dbNanos.get() / 1_000_000L;
    }

    /** @return ms spent in {@code commitSync}, summed over poll threads. */
    public long kafkaCommitMillis() {
        return kafkaCommitNanos.get() / 1_000_000L;
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
