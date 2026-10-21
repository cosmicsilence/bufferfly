package io.bufferfly.spring.ledger;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.backpressure.BackpressureCallback;
import io.bufferfly.core.backpressure.BackpressurePersistenceDispatcher;
import io.bufferfly.core.persistence.NaturalBatchingDispatcher;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActor;
import io.bufferfly.spring.ShardedActorFactory;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link ShardedActor}-annotated factory Spring discovers and wires to a
 * sharded Kafka consumer. Spring instantiates it as a bean (injecting the
 * {@link LedgerOperations} and {@link BalanceConfirmationService}); the
 * {@code ShardedActorBeanPostProcessor} then reads the annotation and stands up
 * one {@link LedgerActor} per partition of the topic.
 *
 * <h2>Backpressure</h2>
 * Each actor's persistence dispatcher is wrapped in a
 * {@link BackpressurePersistenceDispatcher}: when the in-memory batching buffer
 * reaches {@link #highWatermark} the callback calls
 * {@link KafkaContext#pausePartition()} (the broker stops delivering to that
 * partition); once it drains back to {@link #lowWatermark} the callback calls
 * {@link KafkaContext#resumePartition()}. No message is dropped — fetching is
 * simply paused until the database catches up.
 *
 * <p>The dispatcher is given a periodic evaluate interval
 * ({@link #withEvaluateInterval}) so the <em>resume</em> edge fires as the
 * buffer drains even after the partition is paused and no more events are being
 * enqueued; without it a saturated partition would pause and never resume.
 */
@Component
@ShardedActor(name = "ledger", topic = LedgerShardFactory.TOPIC, concurrency = 1,
        highWatermark = 50_000, lowWatermark = 20_000)
public class LedgerShardFactory implements ShardedActorFactory<AddToAccount> {

    public static final String TOPIC = "ledger-events";

    private final LedgerOperations operations;
    private final BalanceConfirmationService confirmations;

    // Backpressure watermarks for the persistence buffer. Default to the
    // annotation values; a test can shrink them via withBackpressure(...).
    private volatile int highWatermark = 50_000;
    private volatile int lowWatermark = 20_000;
    private volatile int batchSize = 200;
    private volatile long timeoutMs = 20L;
    // How often the backpressure dispatcher re-evaluates its buffer depth in the
    // background so the falling edge (resume) fires as the buffer drains even
    // after upstream has been paused and stops enqueuing. A sensible production
    // default; the user tunes it for their release-latency vs. wake-up tradeoff.
    private volatile long evaluateIntervalMs = 50L;

    private final Map<TopicPartition, LedgerActor> actorsByPartition = new ConcurrentHashMap<>();
    private final Set<TopicPartition> halted = ConcurrentHashMap.newKeySet();
    private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();
    private final AtomicInteger restarts = new AtomicInteger();
    private final AtomicInteger pauses = new AtomicInteger();
    private final AtomicInteger resumes = new AtomicInteger();

    public LedgerShardFactory(LedgerOperations operations, BalanceConfirmationService confirmations) {
        this.operations = operations;
        this.confirmations = confirmations;
    }

    /** Tune the persistence/backpressure parameters (used by tests). */
    public LedgerShardFactory withBackpressure(int batchSize, long timeoutMs, int highWatermark, int lowWatermark) {
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
        return this;
    }

    /** Test hook: set how often the backpressure dispatcher re-evaluates its buffer depth (ms). */
    public LedgerShardFactory withEvaluateInterval(long evaluateIntervalMs) {
        this.evaluateIntervalMs = evaluateIntervalMs;
        return this;
    }

    @Override
    public Actor<ConsumedRecord<AddToAccount>> createActor(TopicPartition tp, KafkaContext ctx) {
        // On an unrecoverable failure (e.g. a corrupting record), restart the
        // actor: stop the current instance and start a fresh one whose preStart
        // re-seeks to the authoritative DB-stored offset (forcing an offset
        // rewind), so no committed work is lost and the bad record is replayed.
        Runnable onUnrecoverable = () -> {
            halted.add(tp);
            restarts.incrementAndGet();
            ctx.restartActor();
        };

        // Backpressure: wrap the batching dispatcher so that, when its buffer
        // reaches the high watermark, we pause THIS partition; when it recedes
        // to the low watermark, we resume it. The broker stops delivering to a
        // saturated partition instead of the buffer growing unbounded.
        BackpressureCallback backpressure = new BackpressureCallback() {
            @Override
            public void onBackpressure() {
                paused.add(tp);
                pauses.incrementAndGet();
                ctx.pausePartition();
            }

            @Override
            public void onReleasedPressure() {
                paused.remove(tp);
                resumes.incrementAndGet();
                ctx.resumePartition();
            }
        };
        PersistenceDispatcher<LedgerChange> dispatcher = new BackpressurePersistenceDispatcher<>(
                new NaturalBatchingDispatcher<>(batchSize, timeoutMs),
                highWatermark, lowWatermark, backpressure, evaluateIntervalMs);

        PersistenceConfig<LedgerChange> config = PersistenceConfig.builder(operations)
                .batchSize(batchSize)
                .timeoutMs(timeoutMs)
                .maxRetries(5)
                .dispatcher(dispatcher)
                .onUnrecoverable(onUnrecoverable)
                .build();
        LedgerActor actor = new LedgerActor(actorName(tp), ctx, operations, confirmations, config);
        actorsByPartition.put(tp, actor);
        return actor;
    }

    /** @return {@code true} if the restart hook fired for any partition. */
    public boolean anyHalted() {
        return !halted.isEmpty();
    }

    /** @return how many times an unrecoverable failure triggered an actor restart. */
    public int restartCount() {
        return restarts.get();
    }

    /** @return {@code true} if any partition is currently paused by backpressure. */
    public boolean anyPaused() {
        return !paused.isEmpty();
    }

    /** @return how many times backpressure paused a partition. */
    public int pauseCount() {
        return pauses.get();
    }

    /** @return how many times backpressure resumed a partition. */
    public int resumeCount() {
        return resumes.get();
    }
}
