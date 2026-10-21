package io.bufferfly.core.persistence;

import io.bufferfly.core.actor.ActorDispatcher;

/**
 * Accepts individual persistence events from {@link PersistenceRunner#enqueue}
 * and schedules the consume-batch-flush loop on a virtual thread.
 *
 * <p>Mirrors the role of {@link ActorDispatcher} in the
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
    void enqueue(PersistenceRunner.PersistenceEvent<E> event, PersistenceRunner<E> runner);

    /**
     * Cancels the in-flight consume task and discards any buffered events.
     * Called by {@link PersistenceRunner#stop()}.
     */
    void clear();

    /**
     * Returns the current number of events buffered and awaiting a flush.
     *
     * <p>This is the instantaneous depth of the dispatcher's internal buffer.
     * It is used by backpressure decorators (e.g.
     * {@code BackpressurePersistenceDispatcher}) to compare against configured
     * high/low watermarks. Implementations must make this cheap and
     * non-blocking; the value is a best-effort snapshot and may change
     * concurrently.
     *
     * @return the number of buffered events; never negative
     */
    int size();
}
