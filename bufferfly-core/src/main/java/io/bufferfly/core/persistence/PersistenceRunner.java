package io.bufferfly.core.persistence;

/**
 * Accepts persistence events produced by {@link AbstractPersistentActor} and
 * eventually flushes them to a backing store via {@link TransactionalOperations}.
 *
 * <p>Implementations decouple the actor processing loop (pure in-memory,
 * CPU-bound) from the persistence loop (I/O-bound, blocking network calls).
 * The actor calls {@link #enqueue} from its virtual thread; the runner drains
 * the buffer on its own virtual thread(s) and groups items into batches before
 * handing them to {@link TransactionalOperations#run}.
 *
 * @param <E> the persistence event type produced by {@link AbstractPersistentActor#apply}
 */
public interface PersistenceRunner<E> {

    /**
     * Enqueues a single persistence event. Must be non-blocking so that the
     * actor processing loop is never stalled waiting for I/O.
     *
     * @param event the event to persist; must not be {@code null}
     */
    void enqueue(E event);

    /**
     * Signals that no further events will be enqueued. The runner will flush
     * any remaining buffered events and release its resources.
     */
    void stop();
}
