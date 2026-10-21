package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.backpressure.UnderPressureException;
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
 * {@link #accept(ConsumerRecord, Acknowledgment)} here, keeping all
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
    private final ActorReference<V> actor;
    private final Consumer<TopicPartition> pauseSignal;

    /**
     * @param topicPartition the partition this consumer serves
     * @param actor          the actor owning the partition
     * @param pauseSignal    invoked (with this partition) when the actor rejects
     *                       a record due to backpressure, so the container can
     *                       pause the partition
     */
    public ShardedActorConsumer(TopicPartition topicPartition,
                                ActorReference<V> actor,
                                Consumer<TopicPartition> pauseSignal) {
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition");
        this.actor = Objects.requireNonNull(actor, "actor");
        this.pauseSignal = Objects.requireNonNull(pauseSignal, "pauseSignal");
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Delivers one record's value to the actor and acknowledges it on success.
     *
     * @param record the polled record (its partition must match this consumer's)
     * @param ack    the manual acknowledgment handle
     * @return {@code true} if the record was accepted and acknowledged;
     *         {@code false} if it was rejected due to backpressure (not acked,
     *         partition pause requested, record will be redelivered)
     */
    public boolean accept(ConsumerRecord<?, V> record, Acknowledgment ack) {
        try {
            actor.tell(record.value());
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
            pauseSignal.accept(topicPartition);
            return false;
        }
        // Accepted into the mailbox (and guaranteed to be persisted by the
        // actor's persistence runner) -> safe to acknowledge the offset.
        ack.acknowledge();
        return true;
    }
}
