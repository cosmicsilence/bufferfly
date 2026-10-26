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

    /** An event with no side effects. */
    static <E> PersistenceEvent<E> newEvent(E e) {
        return new PersistenceEvent<>(e, List.of());
    }

    /**
     * An event carrying a single side effect. {@code null} is treated as "no
     * side effect" so callers can pass a nullable value freely.
     */
    static <E> PersistenceEvent<E> newEvent(E e, SideEffect sideEffect) {
        return new PersistenceEvent<>(e, sideEffect == null ? List.of() : List.of(sideEffect));
    }

    /**
     * An event carrying any number of side effects, run in order <em>after</em>
     * the batch holding this event is durably committed.
     */
    @SafeVarargs
    static <E> PersistenceEvent<E> newEvent(E e, SideEffect... sideEffects) {
        return new PersistenceEvent<>(e, (sideEffects == null || sideEffects.length == 0)
                ? List.of() : List.of(sideEffects));
    }

    /** An event carrying a list of side effects (defensively copied). */
    static <E> PersistenceEvent<E> newEvent(E e, List<SideEffect> sideEffects) {
        return new PersistenceEvent<>(e, sideEffects == null ? List.of() : List.copyOf(sideEffects));
    }

    record NoOpSideEffect() implements SideEffect {
    }

    /**
     * A persistence event: the payload {@code event} plus zero or more
     * {@link SideEffect}s to run, in order, <b>after</b> the batch containing it
     * is durably committed. The side-effect list is never {@code null} (empty for
     * none) and is held immutably.
     *
     * @param event       the payload to persist
     * @param sideEffects the post-commit side effects, in run order; never null
     * @param <E>         the persistence event payload type
     */
    record PersistenceEvent<E>(E event, List<SideEffect> sideEffects) {
        public PersistenceEvent {
            sideEffects = (sideEffects == null) ? List.of() : List.copyOf(sideEffects);
        }
    }
}
