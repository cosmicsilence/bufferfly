package io.bufferfly.spring;

import org.apache.kafka.common.TopicPartition;

/**
 * A per-partition handle given to each sharded actor, allowing it to control
 * the Kafka offset of the partition it owns.
 *
 * <p>Because sharded actors are <em>persistent</em> — they write processed data
 * (and the consumed offset) to a database — the authoritative offset lives in
 * the DB, not in Kafka's {@code __consumer_offsets}. When an actor is
 * (re)started for a partition (e.g. after a crash, a rebalance reassignment, or
 * an unrecoverable persistence failure that halted the previous instance) it
 * must resume from the last offset it durably stored.
 *
 * <p>The typical restart flow an actor performs is:
 * <ol>
 *   <li>read the last committed offset for {@link #topicPartition()} from the DB;</li>
 *   <li>call {@link #seek(long)} with {@code storedOffset + 1} (the next record
 *       to process), or {@link #seekToBeginning()} if no offset was stored yet.</li>
 * </ol>
 *
 * <p>All seek operations are applied against the live Kafka consumer that owns
 * this partition. They are only valid while the partition is assigned to this
 * consumer; after a revoke the context is no longer usable.
 *
 * <h2>Threading</h2>
 * Kafka's {@code Consumer} is not thread-safe and must never be called from an
 * actor's (virtual) thread. Every seek here is a <em>signal</em>: it is recorded
 * and relayed to the Kafka consumer thread, which performs the actual seek
 * before its next poll. Callers may invoke these methods freely from the actor
 * thread (e.g. inside {@code preStart()}); a seek requested before the partition
 * is fully wired is buffered and applied as soon as the consumer thread is ready.
 */
public interface KafkaContext {

    /** @return the topic-partition this context (and its actor) is bound to. */
    TopicPartition topicPartition();

    /** Convenience accessor for the topic name. */
    default String topic() {
        return topicPartition().topic();
    }

    /** Convenience accessor for the partition number. */
    default int partition() {
        return topicPartition().partition();
    }

    /**
     * Positions the partition so the next polled record is at {@code offset}.
     * Use the offset of the next <em>unprocessed</em> record (i.e. last durably
     * stored offset + 1).
     *
     * @param offset the absolute offset to resume from; must be &gt;= 0
     */
    void seek(long offset);

    /**
     * Rewinds the partition to its earliest available offset. Used when the
     * actor has no stored offset yet (first run) or must fully replay.
     */
    void seekToBeginning();

    /**
     * Fast-forwards the partition to its latest offset, skipping any backlog.
     * Rarely used by persistent actors; provided for completeness.
     */
    void seekToEnd();

    /**
     * Requests that <em>this partition</em> be paused on the owning Kafka
     * listener container (via the container's {@code pausePartition}). Intended
     * to be called from a persistence-layer
     * {@link io.bufferfly.core.backpressure.BackpressureCallback#onBackpressure()}
     * so that, when this actor's in-memory buffer reaches its high watermark,
     * fetching for this partition stops — without affecting other partitions or
     * actors sharing the container. Idempotent.
     */
    void pausePartition();

    /**
     * Resumes a previously {@link #pausePartition() paused} partition (via the
     * container's {@code resumePartition}). Intended to be called from
     * {@link io.bufferfly.core.backpressure.BackpressureCallback#onReleasedPressure()}
     * once this actor's buffer recedes to its low watermark. Idempotent.
     */
    void resumePartition();

    /**
     * Stops the current actor for this partition and starts a fresh one,
     * forcing an offset rewind.
     *
     * <p>This is the recovery action for an <em>unrecoverable</em> failure — for
     * example a failed offset fence caused by a zombie actor racing the new
     * owner after a rebalance. Stopping tears down the current (possibly
     * contending) instance and its persistence runner; starting creates a new
     * instance whose {@code preStart} re-seeks to the authoritative offset
     * durably stored in the database, so processing resumes from a known-good
     * position with no double counting and no loss.
     *
     * <p>Safe to call from the persistence thread (e.g. a runner's
     * {@code onUnrecoverable} hook); the stop/start is performed off that thread
     * so the failing callback is not restarting itself inline.
     */
    void restartActor();
}
