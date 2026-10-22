package io.bufferfly.spring.resilience;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.backpressure.BackpressureCallback;
import io.bufferfly.core.backpressure.BackpressurePersistenceDispatcher;
import io.bufferfly.core.persistence.NaturalBatchingDispatcher;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActorFactory;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds one {@link MessageActor} per assigned partition and wraps each actor's
 * persistence dispatcher in a {@link BackpressurePersistenceDispatcher} (with the
 * periodic evaluator so the resume edge fires reliably). Backpressure is wired to
 * {@link KafkaContext#pausePartition()} / {@link KafkaContext#resumePartition()}
 * so a saturated partition is paused in isolation — other partitions keep
 * flowing.
 *
 * <p>Shared across the consumers in a group (each consumer has its own container
 * but the same factory type); per-partition pause/resume counters let a test
 * assert which partition engaged backpressure.
 */
public final class MessageShardFactory implements ShardedActorFactory<String> {

    private final MessageOperations operations;

    private final int batchSize;
    private final long timeoutMs;
    private final int highWatermark;
    private final int lowWatermark;
    private final long evaluateIntervalMs;

    private final Map<TopicPartition, MessageActor> actorsByPartition = new ConcurrentHashMap<>();
    private final Set<Integer> pausedPartitions = ConcurrentHashMap.newKeySet();
    private final Map<Integer, AtomicInteger> pausesByPartition = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> resumesByPartition = new ConcurrentHashMap<>();
    private final AtomicInteger createdActors = new AtomicInteger();

    public MessageShardFactory(MessageOperations operations,
                               int batchSize, long timeoutMs,
                               int highWatermark, int lowWatermark,
                               long evaluateIntervalMs) {
        this.operations = operations;
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
        this.evaluateIntervalMs = evaluateIntervalMs;
    }

    @Override
    public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
        createdActors.incrementAndGet();
        int partition = tp.partition();

        // On an unrecoverable failure, restart the actor (fresh preStart rewinds
        // to the stored offset and refreshes the id sequence from the DB).
        Runnable onUnrecoverable = ctx::restartActor;

        BackpressureCallback backpressure = new BackpressureCallback() {
            @Override
            public void onBackpressure() {
                pausedPartitions.add(partition);
                pausesByPartition.computeIfAbsent(partition, p -> new AtomicInteger()).incrementAndGet();
                ctx.pausePartition();
            }

            @Override
            public void onReleasedPressure() {
                pausedPartitions.remove(partition);
                resumesByPartition.computeIfAbsent(partition, p -> new AtomicInteger()).incrementAndGet();
                ctx.resumePartition();
            }
        };

        PersistenceDispatcher<Message> dispatcher = new BackpressurePersistenceDispatcher<>(
                new NaturalBatchingDispatcher<>(batchSize, timeoutMs),
                highWatermark, lowWatermark, backpressure, evaluateIntervalMs);

        PersistenceConfig<Message> config = PersistenceConfig.builder(operations)
                .batchSize(batchSize)
                .timeoutMs(timeoutMs)
                .maxRetries(5)
                .dispatcher(dispatcher)
                .onUnrecoverable(onUnrecoverable)
                .build();

        MessageActor actor = new MessageActor(actorName(tp), partition, ctx, operations, config);
        actorsByPartition.put(tp, actor);
        return actor;
    }

    public boolean isPartitionPaused(int partition) {
        return pausedPartitions.contains(partition);
    }

    public int pauseCount(int partition) {
        AtomicInteger c = pausesByPartition.get(partition);
        return c == null ? 0 : c.get();
    }

    public int resumeCount(int partition) {
        AtomicInteger c = resumesByPartition.get(partition);
        return c == null ? 0 : c.get();
    }

    /** @return how many actors this factory has created (increments on each (re)assignment). */
    public int createdActorCount() {
        return createdActors.get();
    }
}
