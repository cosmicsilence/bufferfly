package io.bufferfly.core.backpressure;

import java.util.Objects;

/**
 * The default {@link BackpressureCallback}: it cascades pressure signals from
 * the persistence layer down to the actor layer.
 *
 * <p>When the monitored
 * {@link io.bufferfly.core.persistence.PersistenceDispatcher} reaches its high
 * watermark, {@link #onBackpressure()} activates the downstream
 * {@link BackpressureActorDispatcher}, which then applies its configured
 * {@link BackpressureStrategy} (CALLER_RUNS or THROW) to every subsequent
 * dispatch. Once the persistence buffer drains back to its low watermark,
 * {@link #onReleasedPressure()} restores normal asynchronous dispatching.
 *
 * <p>The net effect is a feedback loop: a saturated database buffer propagates
 * pressure upstream through the actors to the original producers, either
 * throttling them (CALLER_RUNS) or rejecting them (THROW) until the system
 * catches up.
 */
public final class CascadingBackpressureCallback implements BackpressureCallback {

    private final BackpressureActorDispatcher actorDispatcher;

    public CascadingBackpressureCallback(BackpressureActorDispatcher actorDispatcher) {
        this.actorDispatcher = Objects.requireNonNull(
                actorDispatcher, "actorDispatcher must not be null");
    }

    @Override
    public void onBackpressure() {
        actorDispatcher.applyPressure();
    }

    @Override
    public void onReleasedPressure() {
        actorDispatcher.releasePressure();
    }
}
