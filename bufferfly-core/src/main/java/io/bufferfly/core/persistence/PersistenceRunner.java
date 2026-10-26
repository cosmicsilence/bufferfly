package io.bufferfly.core.persistence;

import java.util.List;

/**
 * Accepts persistence events produced by {@link AbstractPersistentActor} and
 * eventually flushes them to a backing store via {@link TransactionalOperations}.
 *
 * <p>Implementations decouple the actor processing loop (pure in-memory,
 * CPU-bound) from the persistence loop (I/O-bound, blocking network calls).
 * The actor calls {@link #enqueue} from its virtual thread; the runner drains
 * the buffer on its own virtual thread(s) and groups items into batches before
 * handing them to {@link TransactionalOperations#persistOnTransaction}.
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
    void enqueue(PersistenceEvent<E> event);

    /**
     * This is the persistence layer to work with.
     * @param batch - receiving batch of events to be persisted
     */
    void persistBatch(List<PersistenceEvent<E>> batch);


    /**
     * Signals that no further events will be enqueued and stops the runner.
     *
     * <p><b>Buffered events are discarded, not flushed.</b> Any event that was
     * {@link #enqueue enqueued} but not yet part of a <em>committed</em> batch is
     * dropped from memory when {@code stop()} is called (the dispatcher cancels
     * its in-flight drain and clears the buffer). This is intentional and safe
     * under the fencing-token contract: a batch advances the durably-stored
     * offset only when it commits, so discarded-but-uncommitted events were never
     * acknowledged downstream. On restart the actor seeks back to the last
     * committed offset and the source (e.g. Kafka) re-delivers them — nothing is
     * lost <em>end-to-end</em> for a redelivery-backed source.
     *
     * <p>For a source <b>without</b> redelivery (e.g. an in-process producer, or a
     * non-token store), events still buffered at {@code stop()} are lost. There is
     * deliberately no synchronous drain-on-stop: a hard kill cannot guarantee a
     * drain completes, so correctness is anchored to replay, not to flushing.
     */
    void stop();

    /**
     * Marker interface
     */
    interface SideEffect {
    }

    @FunctionalInterface
    interface SideEffectProcessor {
        void process(SideEffect sideEffect);
    }

    static SideEffect noOpSideEffect() {
        return new NoOpSideEffect();
    }

    static <E> PersistenceEvent<E> newEvent(E e) {
        return new PersistenceEvent<>(e, noOpSideEffect());
    }

    static <E> PersistenceEvent<E> newEvent(E e, SideEffect sideEffect) {
        return new PersistenceEvent<>(e, sideEffect);
    }

    record NoOpSideEffect() implements SideEffect {
    }

    record PersistenceEvent<E>(E event, SideEffect sideEffect) {
    }
}
