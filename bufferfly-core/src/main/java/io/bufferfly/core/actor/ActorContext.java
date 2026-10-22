package io.bufferfly.core.actor;

import java.time.Duration;
import java.util.Objects;

/**
 * The capabilities the framework injects into an actor when it starts: a
 * reference to <b>itself</b> ({@link #self()}) and a one-shot self-{@link Scheduler}
 * ({@link #schedule}). Bundling them into a single context means an actor
 * receives exactly one injection ({@link Actor#setContext}) rather than a
 * growing list of setters, and gives a natural home for future per-actor
 * capabilities (ask, watch, periodic timers, …).
 *
 * <p>All members are safe to use from the actor's own thread — e.g. inside
 * {@code preStart} or {@code receive}.
 *
 * @param <T> the actor's message type
 */
public final class ActorContext<T> {

    private final ActorReference<T> self;
    private final Scheduler<T> scheduler;

    public ActorContext(ActorReference<T> self, Scheduler<T> scheduler) {
        this.self = Objects.requireNonNull(self, "self");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** @return a reference to this actor (send it messages, hand it to collaborators, stop it). */
    public ActorReference<T> self() {
        return self;
    }

    /** @return this actor's one-shot self-scheduler. */
    public Scheduler<T> scheduler() {
        return scheduler;
    }

    /**
     * Convenience for {@code scheduler().schedule(message, delay)}: deliver
     * {@code message} to this actor after {@code delay}, on its own thread.
     */
    public void schedule(T message, Duration delay) {
        scheduler.schedule(message, delay);
    }

    /**
     * A context with a real {@code self} but a no-op scheduler. Useful as a
     * default before injection, or in tests.
     */
    public static <T> ActorContext<T> of(ActorReference<T> self) {
        return new ActorContext<>(self, Scheduler.noOp());
    }
}
