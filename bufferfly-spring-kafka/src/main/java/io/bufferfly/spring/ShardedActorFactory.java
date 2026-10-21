package io.bufferfly.spring;

import io.bufferfly.core.actor.Actor;
import org.apache.kafka.common.TopicPartition;

/**
 * Contract a {@link ShardedActor}-annotated bean implements to produce <b>one
 * actor instance per partition</b>.
 *
 * <p>The framework (via {@code ShardedActorBeanPostProcessor} /
 * {@code ShardedActorContainer}) owns the lifecycle: when a partition is
 * assigned it calls {@link #createActor} and starts the returned actor; when the
 * partition is revoked it stops that actor. There is never more than one actor
 * per assigned partition, preserving per-partition ordering.
 *
 * <p>The supplied {@link KafkaContext} is bound to the same partition as the
 * actor. A persistent actor typically uses it during {@code preStart()} to read
 * its last durably stored offset from the database and
 * {@link KafkaContext#seek(long) seek} back to it, guaranteeing no message loss
 * across restarts.
 *
 * <p>Actors returned here consume a {@link ConsumedRecord} wrapping the Kafka
 * <em>value</em> type {@code V} together with its offset; the framework builds
 * the envelope from each {@code ConsumerRecord} and {@code tell}s it to the
 * actor. Each actor's {@link Actor#name()} must be unique per partition —
 * implementations should derive it from the topic-partition (see
 * {@link #actorName(TopicPartition)}).
 *
 * @param <V> the Kafka record value type the actor processes
 */
public interface ShardedActorFactory<V> {

    /**
     * Creates the actor that will own {@code topicPartition}. Called once each
     * time the partition is assigned to this application instance.
     *
     * @param topicPartition the partition this actor is responsible for
     * @param kafkaContext   handle to rewind/seek the partition's offset
     * @return a fresh, not-yet-started actor; the framework calls {@code start()}
     */
    Actor<ConsumedRecord<V>> createActor(TopicPartition topicPartition, KafkaContext kafkaContext);

    /**
     * Derives a unique, stable actor name for a partition. Used by the framework
     * as the registered actor name and mailbox key. Implementations that build
     * their own {@link Actor#name()} should use this to stay consistent.
     *
     * @param topicPartition the partition
     * @return a name of the form {@code <topic>-<partition>}
     */
    default String actorName(TopicPartition topicPartition) {
        return topicPartition.topic() + "-" + topicPartition.partition();
    }
}
