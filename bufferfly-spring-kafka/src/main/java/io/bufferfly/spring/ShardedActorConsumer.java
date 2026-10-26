package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.backpressure.UnderPressureException;
import io.bufferfly.core.metrics.BufferflyMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * The per-partition consumer that bridges a Kafka record to a single BufferFly
 * actor. One instance exists per assigned partition and holds that partition's
 * {@link ActorReference}.
 *
 * <p>The Spring message listener does not talk to the actor directly; it calls
 * {@link #tellActor(ConsumerRecord, Acknowledgment)} here, keeping all
 * actor-interaction and acknowledgement policy in one place.
 *
 * <h2>No message loss</h2>
 * The container runs in {@code MANUAL} / {@code MANUAL_IMMEDIATE} ack mode. A
 * record is acknowledged <strong>only after</strong> the actor has accepted it
 * (i.e. {@code tell} returned, meaning the message is safely in the actor's
 * mailbox and will be processed and persisted). If delivery to the actor fails,
 * the record is <em>not</em> acknowledged, so Kafka will redeliver it after the
 * partition resumes.
 *
 * <h2>Backpressure</h2>
 * When the system is under pressure, {@code tell} throws
 * {@link UnderPressureException} (proactive, THROW strategy) or
 * {@link MailBoxRefusedException} (the actor's bounded mailbox is full). Both
 * are handled identically: we
 * <ul>
 *   <li>do not acknowledge the record (it will be redelivered), and</li>
 *   <li>invoke {@link #pauseSignal} so the owning container pauses this
 *       partition, letting the in-memory buffer drain and the system heal.</li>
 * </ul>
 * Once the buffer recedes below the low watermark the container resumes the
 * partition and the un-acked record is redelivered — no message is lost.
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
     * Delivers one record (value + offset envelope) to the actor and
     * acknowledges it on success.
     *
     * @param record the polled record (its partition must match this consumer's)
     * @param ack    the manual acknowledgment handle
     * @return {@code true} if the record was accepted and acknowledged;
     *         {@code false} if it was rejected due to backpressure (not acked,
     *         partition pause requested, record will be redelivered)
     */
    public boolean tellActor(ConsumerRecord<?, V> record, Acknowledgment ack) {
        ConsumedRecord<V> envelope = new ConsumedRecord<>(
                record.topic(), record.partition(), record.offset(), record.value());
        try {
            actor.tell(envelope);
        } catch (UnderPressureException | MailBoxRefusedException bp) {
            // Do NOT acknowledge: the record stays uncommitted and will be
            // redelivered once the partition is resumed. Ask the container to
            // pause this partition so the system can drain and heal.
            //
            // UnderPressureException  -> proactive backpressure (THROW strategy).
            // MailBoxRefusedException -> the actor's bounded mailbox is full.
            // Both mean "stop feeding me"; they are handled identically.
            log.warn("Backpressure ({}) on {}-{} at offset {}; pausing partition for redelivery",
                    bp.getClass().getSimpleName(), record.topic(), record.partition(), record.offset());
            metrics.recordRejected(component);
            pauseSignal.accept(topicPartition);
            return false;
        }
        // Accepted into the mailbox. We acknowledge the Kafka offset here, but
        // note this is NOT a guarantee that persistence succeeds: the record is
        // processed asynchronously by the actor and its persistence runner.
        //
        // Durability is instead guaranteed by the actor persisting the consumed
        // offset in the SAME transaction as the data (see the ledger showcase's
        // optimistic-locking fencing token). The Kafka ack only governs where a
        // fresh consumer would resume; on restart the actor seeks back to its
        // DB-stored offset regardless, so nothing committed to Kafka but not to
        // the DB is lost.
        //
        // If persistence rejects a record as a poison pill, the runner isolates
        // it (TransactionalOperations.onPoisonPill) — e.g. writing it to a
        // DEAD_MESSAGE(id, payload, topic, partition) table — and may then halt
        // the actor; the container restarts it, and it resumes from the last
        // durably stored offset, skipping the dead-lettered record without loss.
        ack.acknowledge();
        return true;
    }
}
