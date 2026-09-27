package io.bufferfly.core.persistence;

/**
 * Accepts individual persistence events from {@link PersistenceRunner#enqueue}
 * and schedules the consume-batch-flush loop on a virtual thread.
 *
 * <p>Mirrors the role of {@link io.bufferfly.core.actor.Dispatcher} in the
 * actor layer: it owns the virtual thread scheduling and the buffer, while
 * {@link BatchingPersistenceRunner} owns the business logic (batching config,
 * error handling, retry policy).
 *
 * @param <E> the persistence event type
 */
public interface PersistenceDispatcher<E> {

    /**
     * Accepts one event into the buffer and ensures the consume loop is
     * scheduled. Must be non-blocking.
     */
    void enqueue(E event, BatchingPersistenceRunner<E> runner);

    /**
     * Cancels the in-flight consume task and discards any buffered events.
     * Called by {@link PersistenceRunner#stop()}.
     */
    void clear();
}
