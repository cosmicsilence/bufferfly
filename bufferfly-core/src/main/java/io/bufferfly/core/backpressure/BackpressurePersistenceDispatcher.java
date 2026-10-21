package io.bufferfly.core.backpressure;

import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.core.persistence.PersistenceRunner;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link PersistenceDispatcher} decorator that monitors the wrapped
 * dispatcher's buffer depth and fires a {@link BackpressureCallback} as the
 * depth crosses configured watermarks.
 *
 * <p>On every {@link #enqueue} the delegate is called first, then
 * {@link PersistenceDispatcher#size()} is inspected:
 * <ul>
 *   <li>when {@code size >= highWatermark} and we are not already under
 *       pressure, {@link BackpressureCallback#onBackpressure()} fires once
 *       (rising edge) and the state flips to "under pressure";</li>
 *   <li>when {@code size <= lowWatermark} and we <em>are</em> under pressure,
 *       {@link BackpressureCallback#onReleasedPressure()} fires once (falling
 *       edge) and the state flips back to "relieved".</li>
 * </ul>
 *
 * <p>Using two distinct watermarks (high &gt; low) provides hysteresis,
 * preventing callback thrashing when the buffer depth oscillates around a
 * single threshold.
 *
 * <p>State transitions are guarded by an {@link AtomicBoolean} so each edge
 * fires exactly once even under concurrent enqueues.
 *
 * @param <E> the persistence event type
 */
public final class BackpressurePersistenceDispatcher<E> implements PersistenceDispatcher<E> {

    private final PersistenceDispatcher<E> delegate;
    private final int highWatermark;
    private final int lowWatermark;
    private final BackpressureCallback callback;

    private final AtomicBoolean underPressure = new AtomicBoolean(false);

    /**
     * @param delegate      the dispatcher whose buffer depth is monitored
     * @param highWatermark buffer depth at which backpressure is signalled; must be &gt; {@code lowWatermark}
     * @param lowWatermark  buffer depth at which pressure is released; must be &gt;= 0
     * @param callback      receives the edge-triggered transitions
     */
    public BackpressurePersistenceDispatcher(PersistenceDispatcher<E> delegate,
                                             int highWatermark,
                                             int lowWatermark,
                                             BackpressureCallback callback) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.callback = Objects.requireNonNull(callback, "callback must not be null");
        if (lowWatermark < 0)
            throw new IllegalArgumentException("lowWatermark must be >= 0, got " + lowWatermark);
        if (highWatermark <= lowWatermark)
            throw new IllegalArgumentException(
                    "highWatermark (%d) must be > lowWatermark (%d)".formatted(highWatermark, lowWatermark));
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public void enqueue(PersistenceRunner.PersistenceEvent<E> event, PersistenceRunner<E> runner) {
        delegate.enqueue(event, runner);
        evaluate(delegate.size());
    }

    @Override
    public void clear() {
        delegate.clear();
        // The buffer is now empty; relieve pressure if we were holding it.
        if (underPressure.compareAndSet(true, false)) {
            callback.onReleasedPressure();
        }
    }

    @Override
    public int size() {
        return delegate.size();
    }

    /**
     * Compares the current buffer depth against the watermarks and fires the
     * appropriate edge-triggered callback. Package-private for testing.
     */
    void evaluate(int currentSize) {
        if (currentSize >= highWatermark) {
            if (underPressure.compareAndSet(false, true)) {
                callback.onBackpressure();
            }
        } else if (currentSize <= lowWatermark) {
            if (underPressure.compareAndSet(true, false)) {
                callback.onReleasedPressure();
            }
        }
    }

    /** @return {@code true} if currently in the under-pressure state. */
    public boolean isUnderPressure() {
        return underPressure.get();
    }

    public int highWatermark() {
        return highWatermark;
    }

    public int lowWatermark() {
        return lowWatermark;
    }
}
