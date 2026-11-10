package io.bufferfly.spring.rewind;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActorFactory;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds one {@link RewindActor} per assigned partition, wiring the actor's
 * {@code onUnrecoverable} hook to {@code ctx.restartActor()} so an injected
 * mid-batch failure on partition 0 triggers a real restart + offset rewind,
 * while partition 1 is untouched.
 *
 * <p>Per-partition counters/sinks let the test observe (a) how many actors were
 * created per partition — a restart bumps only partition 0 — and (b) every
 * {@code (offset,value)} each partition applied, so the replayed P0 slice is
 * visible.
 */
public final class RewindShardFactory implements ShardedActorFactory<String> {

    private final RewindOperations operations;
    private final int batchSize;

    private final Map<Integer, AtomicInteger> actorsCreatedByPartition = new ConcurrentHashMap<>();
    private final Map<Integer, List<String>> appliedByPartition = new ConcurrentHashMap<>();

    public RewindShardFactory(RewindOperations operations, int batchSize) {
        this.operations = operations;
        this.batchSize = batchSize;
    }

    @Override
    public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
        int partition = tp.partition();
        actorsCreatedByPartition.computeIfAbsent(partition, p -> new AtomicInteger()).incrementAndGet();
        List<String> appliedSink =
                appliedByPartition.computeIfAbsent(partition, p -> new CopyOnWriteArrayList<>());

        PersistenceConfig<RewindMessage> config = PersistenceConfig.builder(operations)
                .batchSize(batchSize)
                .maxRetries(3)
                .onUnrecoverable(ctx::restartActor)
                .build();

        return new RewindActor(actorName(tp), partition, ctx, operations, config, appliedSink);
    }

    /** @return how many actor instances were created for a partition (restart => >1 for P0). */
    public int actorsCreated(int partition) {
        AtomicInteger c = actorsCreatedByPartition.get(partition);
        return c == null ? 0 : c.get();
    }

    /** @return every (partition:offset:value) applied by a partition's actor(s), in order. */
    public List<String> applied(int partition) {
        return appliedByPartition.getOrDefault(partition, List.of());
    }
}
