package io.bufferfly.core.backpressure;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorDispatcher;
import io.bufferfly.core.actor.CallerRunsCapable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A decorator over an {@link ActorDispatcher} that changes its dispatch
 * behavior while the system is <em>under pressure</em>.
 *
 * <p>Under normal conditions every {@link #dispatch} is forwarded unchanged to
 * the wrapped {@code delegate} (typically the asynchronous
 * {@link io.bufferfly.core.actor.VTDispatcher}). Once pressure is signalled via
 * {@link #applyPressure()} the configured {@link BackpressureStrategy} takes
 * over:
 *
 * <ul>
 *   <li>{@link BackpressureStrategy#CALLER_RUNS} — the delegate is switched into
 *       caller-runs mode via {@link CallerRunsCapable#setCallerRuns(boolean)}.
 *       The delegate then drains the actor's <em>existing mailbox</em> on the
 *       caller thread rather than on a virtual thread. This hijacks the
 *       producer's thread <strong>without</strong> reordering messages or
 *       breaking the single-threaded-actor invariant, because the delegate
 *       keeps using the same mailbox and single-consumer slot. Requires the
 *       delegate to implement {@link CallerRunsCapable}.</li>
 *   <li>{@link BackpressureStrategy#THROW} — dispatches are rejected with an
 *       {@link UnderPressureException} before reaching the delegate.</li>
 * </ul>
 *
 * <p>{@link #releasePressure()} restores normal forwarding. The pressure flag
 * is toggled by a {@link BackpressureCallback} (see
 * {@link CascadingBackpressureCallback}) wired to a monitored
 * {@link io.bufferfly.core.persistence.PersistenceDispatcher}.
 */
public final class BackpressureActorDispatcher implements ActorDispatcher {

    private final ActorDispatcher delegate;
    private final BackpressureStrategy strategy;
    private final AtomicBoolean underPressure = new AtomicBoolean(false);

    public BackpressureActorDispatcher(ActorDispatcher delegate, BackpressureStrategy strategy) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
        if (strategy == BackpressureStrategy.CALLER_RUNS && !(delegate instanceof CallerRunsCapable)) {
            throw new IllegalArgumentException(
                    "CALLER_RUNS strategy requires a delegate implementing CallerRunsCapable; "
                            + "high-performance dispatchers (e.g. SpinningActorDispatcher) intentionally "
                            + "support only the THROW strategy. Got: " + delegate.getClass().getName());
        }
    }

    /**
     * Marks the dispatcher as under pressure, activating the strategy. For
     * {@link BackpressureStrategy#CALLER_RUNS} this flips the delegate into
     * caller-runs mode so subsequent drains hijack the caller thread.
     */
    public void applyPressure() {
        if (underPressure.compareAndSet(false, true)
                && strategy == BackpressureStrategy.CALLER_RUNS) {
            ((CallerRunsCapable) delegate).setCallerRuns(true);
        }
    }

    /**
     * Clears the pressure flag, restoring normal asynchronous forwarding. For
     * {@link BackpressureStrategy#CALLER_RUNS} this flips the delegate back to
     * virtual-thread mode.
     */
    public void releasePressure() {
        if (underPressure.compareAndSet(true, false)
                && strategy == BackpressureStrategy.CALLER_RUNS) {
            ((CallerRunsCapable) delegate).setCallerRuns(false);
        }
    }

    /** @return {@code true} if the dispatcher is currently under pressure. */
    public boolean isUnderPressure() {
        return underPressure.get();
    }

    /** @return the configured strategy. */
    public BackpressureStrategy strategy() {
        return strategy;
    }

    @Override
    public <T> void dispatch(T t, Actor<T> actor) {
        // THROW must reject before the message ever enters the mailbox.
        if (strategy == BackpressureStrategy.THROW && underPressure.get()) {
            throw new UnderPressureException(
                    "Dispatch rejected: system under pressure (actor.name=%s)"
                            .formatted(actor.name()));
        }
        // CALLER_RUNS: the delegate itself decides (async vs caller-runs) based
        // on the mode we toggled in applyPressure/releasePressure. Ordering and
        // the single-consumer invariant are preserved by the delegate.
        delegate.dispatch(t, actor);
    }

    @Override
    public <T> void clear(Actor<T> actor) {
        delegate.clear(actor);
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}
