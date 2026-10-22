package io.bufferfly.spring.persistence;

/**
 * A persistence event that carries the Kafka coordinates it originated from.
 *
 * <p>{@link AbstractJdbcTransactionalOperations} uses these coordinates to
 * advance the stored consumer offset as an optimistic-locking <b>fencing
 * token</b> (see {@link FencingOffsetRepository}) in the same transaction as
 * the data. Because one actor owns one partition, every event in a given batch
 * shares the same {@code topic} and {@code partition}; the repository advances
 * the token to the <em>highest offset in the batch</em>.
 *
 * <p>Implement this on your persistence-event type so the base class can derive
 * the fencing coordinates generically, without each operations class
 * re-implementing the fence-and-advance plumbing.
 */
public interface OffsetBearing {

    /** @return the source Kafka topic. */
    String topic();

    /** @return the source Kafka partition. */
    int partition();

    /** @return the source Kafka offset within the partition (the fencing token component). */
    long offset();
}
