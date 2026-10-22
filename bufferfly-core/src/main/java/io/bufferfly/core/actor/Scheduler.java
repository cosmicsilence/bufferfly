package io.bufferfly.core.actor;

import java.time.Duration;

/**
 * An actor's capability to schedule a message to <b>itself</b> after a delay.
 *
 * <p>Like {@code self}, a {@code Scheduler} is injected into every actor during
 * {@link Actor#start()} (see {@link ActorManager}). It lets an actor arm a
 * one-shot timer without touching threads, locks, or executors directly: it
 * captures the actor's own {@link ActorReference} and, after the delay elapses,
 * performs a normal {@code self.tell(message)} — so the delayed message is
 * processed on the actor's own single thread, preserving the single-writer
 * invariant. Timers therefore need no synchronization in actor code.
 *
 * <p>The scheduling thread is shared across all actors (a small pool owned by
 * the {@link ActorManager}); only the cheap {@code self.tell} hand-off runs
 * there, never actor business logic.
 *
 * <p>One-shot only for now (no periodic scheduling). If the actor has stopped by
 * the time the delay elapses, the delivery is silently dropped (a stopped actor
 * must not receive messages).
 *
 * @param <T> the actor's message type
 */
public interface Scheduler<T> {

    /**
     * Schedules {@code message} to be delivered to this actor (via
     * {@code self.tell}) once {@code delay} has elapsed. Non-blocking; returns
     * immediately. The message is delivered at most once.
     *
     * @param message the message to deliver to self; must not be {@code null}
     * @param delay   how long to wait before delivery; must not be negative
     */
    void schedule(T message, Duration delay);

    /** A scheduler that does nothing. Useful as a default or in tests. */
    @SuppressWarnings("rawtypes")
    Scheduler NO_OP = (message, delay) -> {
    };

    @SuppressWarnings("unchecked")
    static <T> Scheduler<T> noOp() {
        return (Scheduler<T>) NO_OP;
    }
}
