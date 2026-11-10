package io.bufferfly.spring.benchmark.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.persistence.AbstractPersistentActor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActorContainer;
import io.bufferfly.spring.ShardedActorFactory;
import io.bufferfly.spring.benchmark.BenchmarkConsumers;
import io.bufferfly.spring.benchmark.PgActorLedgerOperations;
import io.bufferfly.spring.benchmark.PgLedgerChange;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The BufferFly side of the Flink benchmark, using the real Spring integration:
 * one {@link ShardedActorContainer} on the topic with one consumer per partition
 * ({@code concurrency = partitions}), each partition owned by a persistent
 * ledger actor.
 *
 * <p>Each actor keeps its partition's balances in a plain {@code HashMap} (in
 * memory, single writer, no locks) and hands one {@link PgLedgerChange} per
 * record to the natural-batching persistence runner, which writes up to
 * {@code batchSize} changes per transaction through
 * {@link PgActorLedgerOperations}.
 *
 * <p>The actor is restart-safe, as production code must be: on every
 * (re)assignment its {@code preStart} loads the partition's balances and stored
 * offset from the database and seeks there, and an unrecoverable persistence
 * failure (e.g. a failed offset fence after a rebalance) restarts it. The
 * consumers of the group join at slightly different times, so a partition can
 * move once at start-up; this path makes that move lossless.
 */
public final class ShardedLedgerRunner implements AutoCloseable {

    private final String bootstrap;
    private final String topic;
    private final int partitions;
    private final String groupId;
    private final PgActorLedgerOperations operations;
    private final int batchSize;

    private final ActorManager manager = new ActorManager();
    private final AtomicLong firstEventNanos = new AtomicLong();
    private ShardedActorContainer<String, String> container;

    public ShardedLedgerRunner(String bootstrap, String topic, int partitions, String groupId,
                               PgActorLedgerOperations operations, int batchSize) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.partitions = partitions;
        this.groupId = groupId;
        this.operations = operations;
        this.batchSize = batchSize;
    }

    /** @return wall time from start, and steady time from the first record, to all changes committed */
    public FlinkLedgerJob.Result runUntil(long expectedTotal) throws InterruptedException {
        long start = System.nanoTime();
        container = new ShardedActorContainer<>(groupId, topic, partitions, new LedgerFactory(),
                consumerFactory(), manager);
        container.start();
        while (operations.persisted() < expectedTotal) {
            Thread.sleep(5);
        }
        long end = System.nanoTime();
        return new FlinkLedgerJob.Result((end - start) / 1_000_000L,
                (end - firstEventNanos.get()) / 1_000_000L, partitions);
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, BenchmarkConsumers.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private final class LedgerFactory implements ShardedActorFactory<String> {
        @Override
        public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
            PersistenceConfig<PgLedgerChange> config = PersistenceConfig.builder(operations)
                    .batchSize(batchSize)
                    .maxRetries(3)
                    .onUnrecoverable(ctx::restartActor)
                    .componentName(actorName(tp))
                    .build();
            return new LedgerActor(actorName(tp), ctx, operations, config, firstEventNanos);
        }
    }

    /** Parses each record, updates the in-memory balance, and persists the change. */
    static final class LedgerActor extends AbstractPersistentActor<ConsumedRecord<String>, PgLedgerChange> {
        private final String name;
        private final KafkaContext ctx;
        private final PgActorLedgerOperations operations;
        private final AtomicLong firstEventNanos;
        private final ObjectMapper mapper = new ObjectMapper();
        private final Map<String, Long> balances = new HashMap<>();

        LedgerActor(String name, KafkaContext ctx, PgActorLedgerOperations operations,
                    PersistenceConfig<PgLedgerChange> config, AtomicLong firstEventNanos) {
            super(config);
            this.name = name;
            this.ctx = ctx;
            this.operations = operations;
            this.firstEventNanos = firstEventNanos;
        }

        @Override
        protected void preStart() {
            // Restart-safe start: resume from the state and offset durably stored
            // for this partition, so a partition that moved to this consumer (or a
            // restarted actor) neither loses nor double-counts records.
            balances.putAll(operations.store().balancesForPartition(ctx.topic(), ctx.partition()));
            long stored = operations.loadStoredOffset(ctx.topic(), ctx.partition());
            initToken(stored);
            if (stored < 0) {
                ctx.seekToBeginning();
            } else {
                ctx.seek(stored + 1);
            }
        }

        @Override
        public PersistenceRunner.PersistenceEvent<PgLedgerChange> apply(ConsumedRecord<String> record) {
            firstEventNanos.compareAndSet(0L, System.nanoTime());
            AddToAccount cmd;
            try {
                cmd = mapper.readValue(record.value(), AddToAccount.class);
            } catch (Exception e) {
                throw new IllegalStateException("bad ledger record at " + record.offset(), e);
            }
            long balance = balances.merge(cmd.accountNumber(), cmd.amount(), Long::sum);
            return PersistenceRunner.newEvent(new PgLedgerChange(record.topic(), record.partition(),
                    record.offset(), cmd.accountNumber(), cmd.amount(), balance));
        }

        @Override
        public PersistenceRunner.SideEffectProcessor process() {
            return sideEffect -> { };
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onError(ConsumedRecord<String> record, Exception e) {
            throw new IllegalStateException("ledger actor " + name + " failed", e);
        }
    }

    @Override
    public void close() {
        if (container != null) {
            container.stop();
        }
        manager.shutdown();
    }
}
