package io.bufferfly.core.actor;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default {@link Scheduler} implementation: after the delay elapses on a shared
 * {@link ScheduledExecutorService}, it performs {@code self.tell(message)} so the
 * message is processed on the actor's own thread.
 *
 * <p>One instance is created per actor at start time, capturing that actor's
 * {@link ActorReference}. The executor is shared and owned by the
 * {@link ActorManager}; the scheduled task does nothing but the cheap
 * {@code self.tell} hand-off.
 *
 * @param <T> the actor's message type
 */
final class SelfTellScheduler<T> implements Scheduler<T> {

    private final ActorReference<T> self;
    private final ScheduledExecutorService executor;

    SelfTellScheduler(ActorReference<T> self, ScheduledExecutorService executor) {
        this.self = Objects.requireNonNull(self, "self");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public void schedule(T message, Duration delay) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative: " + delay);
        }
        executor.schedule(() -> {
            try {
                self.tell(message);
            } catch (ActorNotStartedException stopped) {
                // The actor stopped before the timer fired: drop the delivery.
                // A stopped actor must not receive messages.
            } catch (RuntimeException ignored) {
                // Never let a scheduled hand-off kill the shared scheduler thread.
            }
        }, Math.max(0L, delay.toMillis()), TimeUnit.MILLISECONDS);
    }
}
