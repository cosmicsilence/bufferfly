package io.bufferfly.spring;

import org.apache.kafka.common.TopicPartition;

/**
 * The unit of work delivered to a sharded actor: a Kafka record's value paired
 * with its source coordinates (topic, partition, offset).
 *
 * <p>Sharded actors are persistent and typically need the <em>offset</em>, not
 * just the value, so they can durably record how far they have consumed — for
 * example persisting the latest processed offset as an optimistic-locking
 * fencing token alongside the data in the same transaction. Delivering this
 * envelope (rather than the bare value) keeps that offset available inside the
 * actor's {@code apply}/{@code receive} without the actor needing access to the
 * Kafka consumer.
 *
 * @param <V>       the Kafka record value type
 * @param topic     source topic
 * @param partition source partition
 * @param offset    the record's offset within the partition
 * @param value     the deserialized record value
 */
public record ConsumedRecord<V>(String topic, int partition, long offset, V value) {

    /** @return the {@link TopicPartition} this record came from. */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}
