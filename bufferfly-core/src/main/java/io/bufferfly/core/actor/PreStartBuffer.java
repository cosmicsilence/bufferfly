package io.bufferfly.core.actor;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Defers a {@link Runnable} action for actors that are still in
 * {@link AbstractActor.State#PRE_STARTING} (or {@link AbstractActor.State#NOT_STARTED}).
 *
 * <p>Once the actor transitions to {@code STARTED}, the buffered action
 * is executed. Re-checks happen on a 50 ms schedule so the delay is bounded.
 *
 * <p>Thread-safe — designed to be shared across dispatcher implementations
 * ({@link VTDispatcher}, {@code SpinningActorDispatcher}, …).
 */
public class PreStartBuffer {

    private static final long RETRY_DELAY_MS = 50;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, ScheduledFuture<?>> pending = new ConcurrentHashMap<>();

    /**
     * If the actor is still pre-starting, schedules {@code action} for later
     * execution and returns {@code true}. Otherwise returns {@code false} and
     * the caller should proceed normally.
     */
    public boolean bufferIfPreStarting(Actor<?> actor, Runnable action) {
        if (!actor.isPreStarting()) return false;
        pending.putIfAbsent(actor.name(), scheduler.schedule(() -> {
            pending.remove(actor.name());
            action.run();
        }, RETRY_DELAY_MS, TimeUnit.MILLISECONDS));
        return true;
    }

    public void remove(String actorName) {
        var future = pending.remove(actorName);
        if (future != null) future.cancel(false);
    }

    public void stop() {
        pending.clear();
        scheduler.shutdown();
    }
}
