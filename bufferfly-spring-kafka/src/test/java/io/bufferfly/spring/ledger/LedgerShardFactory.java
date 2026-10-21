package io.bufferfly.spring.ledger;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActor;
import io.bufferfly.spring.ShardedActorFactory;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link ShardedActor}-annotated factory Spring discovers and wires to a
 * sharded Kafka consumer. Spring instantiates it as a bean (injecting the
 * {@link LedgerOperations} and {@link BalanceConfirmationService}); the
 * {@code ShardedActorBeanPostProcessor} then reads the annotation and stands up
 * one {@link LedgerActor} per partition of the topic.
 */
@Component
@ShardedActor(name = "ledger", topic = LedgerShardFactory.TOPIC, concurrency = 1,
        highWatermark = 50_000, lowWatermark = 20_000)
public class LedgerShardFactory implements ShardedActorFactory<AddToAccount> {

    public static final String TOPIC = "ledger-events";

    private final LedgerOperations operations;
    private final BalanceConfirmationService confirmations;

    private final Map<TopicPartition, LedgerActor> actorsByPartition = new ConcurrentHashMap<>();
    private final Set<TopicPartition> halted = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicInteger restarts = new java.util.concurrent.atomic.AtomicInteger();

    public LedgerShardFactory(LedgerOperations operations, BalanceConfirmationService confirmations) {
        this.operations = operations;
        this.confirmations = confirmations;
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
        PersistenceConfig<LedgerChange> config = PersistenceConfig.builder(operations)
                .batchSize(200)
                .timeoutMs(20L)
                .maxRetries(5)
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
}
