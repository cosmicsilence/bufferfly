package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.backpressure.UnderPressureException;
import io.bufferfly.core.metrics.BufferflyMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * The per-partition consumer that bridges a Kafka record to a single BufferFly
 * actor. One instance exists per assigned partition and holds that partition's
 * {@link ActorReference}.
 *
 * <p>The Spring message listener does not talk to the actor directly; it calls
 * {@link #tellActor(ConsumerRecord)} here, keeping all actor-interaction policy
 * in one place. Offset commits are owned by the container, which commits a
 * partition only when its whole slice of a poll was accepted.
 *
 * <h2>Backpressure</h2>
 * When the system is under pressure, {@code tell} throws
 * {@link UnderPressureException} (proactive, THROW strategy) or
 * {@link MailBoxRefusedException} (the actor's bounded mailbox is full). Both
 * are handled identically: we report the record as not accepted (so the
 * container does not commit the partition) and invoke {@link #pauseSignal} so
 * the owning container pauses this partition, letting the in-memory buffer drain.
 *
 * @param <V> the Kafka record value type
 */
public final class ShardedActorConsumer<V> {

    private static final Logger log = LoggerFactory.getLogger(ShardedActorConsumer.class);

    private final TopicPartition topicPartition;
    private final ActorReference<ConsumedRecord<V>> actor;
    private final Consumer<TopicPartition> pauseSignal;
    private final BufferflyMetrics metrics;
    private final String component;

    /**
     * @param topicPartition the partition this consumer serves
     * @param actor          the actor owning the partition
     * @param pauseSignal    invoked (with this partition) when the actor rejects
     *                       a record due to backpressure, so the container can
     *                       pause the partition
     */
    public ShardedActorConsumer(TopicPartition topicPartition,
                                ActorReference<ConsumedRecord<V>> actor,
                                Consumer<TopicPartition> pauseSignal) {
        this(topicPartition, actor, pauseSignal, BufferflyMetrics.NOOP, null);
    }

    /**
     * @param topicPartition the partition this consumer serves
     * @param actor          the actor owning the partition
     * @param pauseSignal    invoked when the actor rejects a record under pressure
     * @param metrics        observability sink; {@link BufferflyMetrics#NOOP} to disable
     * @param component      the component label (the per-partition actor name)
     */
    public ShardedActorConsumer(TopicPartition topicPartition,
                                ActorReference<ConsumedRecord<V>> actor,
                                Consumer<TopicPartition> pauseSignal,
                                BufferflyMetrics metrics,
                                String component) {
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition");
        this.actor = Objects.requireNonNull(actor, "actor");
        this.pauseSignal = Objects.requireNonNull(pauseSignal, "pauseSignal");
        this.metrics = metrics == null ? BufferflyMetrics.NOOP : metrics;
        this.component = component == null ? topicPartition.toString() : component;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Delivers one record (value + offset envelope) to the actor.
     *
     * <p>Acceptance only means the record is in the actor's mailbox; durability
     * is guaranteed by the actor persisting the consumed offset in the same
     * transaction as the data (the fencing token). On restart the actor seeks
     * back to its DB-stored offset, so a Kafka commit that ran ahead of the DB
     * loses nothing.
     *
     * @param record the polled record (its partition must match this consumer's)
     * @return {@code true} if the actor accepted the record; {@code false} if it
     *         was rejected due to backpressure (partition pause requested)
     */
    public boolean tellActor(ConsumerRecord<?, V> record) {
        ConsumedRecord<V> envelope = new ConsumedRecord<>(
                record.topic(), record.partition(), record.offset(), record.value());
        try {
            actor.tell(envelope);
            return true;
        } catch (UnderPressureException | MailBoxRefusedException bp) {
            // UnderPressureException  -> proactive backpressure (THROW strategy).
            // MailBoxRefusedException -> the actor's bounded mailbox is full.
            // Both mean "stop feeding me"; they are handled identically.
            log.warn("Backpressure ({}) on {}-{} at offset {}; pausing partition",
                    bp.getClass().getSimpleName(), record.topic(), record.partition(), record.offset());
            metrics.recordRejected(component);
            pauseSignal.accept(topicPartition);
            return false;
        }
    }
}
