package io.bufferfly.core.backpressure;

/**
 * Receives transitions of a buffered component (typically a
 * {@link io.bufferfly.core.persistence.PersistenceDispatcher}) across its
 * configured watermarks.
 *
 * <p>A {@link BackpressurePersistenceDispatcher} inspects its wrapped
 * dispatcher's {@code size()} on every enqueue and fires:
 * <ul>
 *   <li>{@link #onBackpressure()} once, when the buffer depth first reaches the
 *       high watermark (rising edge);</li>
 *   <li>{@link #onReleasedPressure()} once, when the buffer depth then recedes
 *       to the low watermark (falling edge).</li>
 * </ul>
 *
 * <p>Callbacks are <strong>edge-triggered</strong>: they fire only on the
 * transition, not on every call while over/under a threshold. Implementations
 * must be thread-safe and must not block for long — they run on the caller's
 * enqueue path.
 *
 * <p>The default implementation,
 * {@link CascadingBackpressureCallback}, cascades the signal from the
 * persistence layer down to an actor dispatcher so that upstream producers are
 * slowed (CALLER_RUNS) or rejected (THROW) until the persistence buffer drains.
 */
public interface BackpressureCallback {

    /**
     * Invoked once when the monitored buffer rises to its high watermark.
     * Marks the system as being "under pressure".
     */
    void onBackpressure();

    /**
     * Invoked once when the monitored buffer, having been under pressure,
     * recedes back down to its low watermark. Marks pressure as relieved.
     */
    void onReleasedPressure();

    /** A callback that does nothing. Useful as a default or in tests. */
    BackpressureCallback NO_OP = new BackpressureCallback() {
        @Override
        public void onBackpressure() {
        }

        @Override
        public void onReleasedPressure() {
        }
    };
}
