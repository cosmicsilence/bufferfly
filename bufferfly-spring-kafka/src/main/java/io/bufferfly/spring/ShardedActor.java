package io.bufferfly.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an actor type as a <em>sharded</em> consumer bound to a Kafka topic.
 *
 * <p>A sharded actor is fanned out into {@link #concurrency()} independent
 * shards, each consuming a disjoint subset of the topic's partitions and
 * processing them through its own BufferFly actor instance. This preserves
 * per-key ordering within a partition while scaling horizontally across cores.
 *
 * <p>The watermarks configure the backpressure feedback loop that BufferFly
 * places in front of the persistence buffer: when the in-memory buffer depth
 * reaches {@link #highWatermark()} the framework signals backpressure
 * (throttling or rejecting upstream production); once it recedes to
 * {@link #lowWatermark()} normal flow resumes. {@code highWatermark} must be
 * strictly greater than {@code lowWatermark}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedActor {

    /** Logical name of the actor; used to identify and register its shards. */
    String name();

    /** Kafka topic this actor consumes from. */
    String topic();

    /** Number of shards (consumer concurrency) to fan the topic out into. */
    int concurrency() default 3;

    /** Buffer depth at which backpressure is engaged. Must be &gt; {@link #lowWatermark()}. */
    int highWatermark() default 50_000;

    /** Buffer depth at which backpressure is released. */
    int lowWatermark() default 20_000;
}
