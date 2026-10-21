package io.bufferfly.core.backpressure;

import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.core.persistence.PersistenceRunner;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
 * <h2>Periodic evaluation (the falling edge problem)</h2>
 * Evaluating only on {@link #enqueue} is enough to <em>engage</em> backpressure
 * (the rising edge happens precisely while events are arriving), but it cannot
 * reliably <em>release</em> it: once the high watermark pauses upstream, new
 * events stop arriving, so there are no more enqueues to observe the buffer
 * draining back below the low watermark — the falling edge would never fire and
 * the partition would stay paused forever.
 *
 * <p>To close that gap this dispatcher can run a <b>periodic evaluator</b>: when
 * constructed with a positive {@code evaluateIntervalMs}, a single-threaded
 * daemon scheduler calls {@link #evaluate(int)} every interval, so the falling
 * edge fires on a tick as the buffer drains even while no enqueues happen. The
 * interval is caller-configurable: a test may use a tiny window for fast,
 * deterministic transitions; in production the user chooses a value balancing
 * backpressure-release latency against wake-up overhead. Passing {@code 0}
 * disables the scheduler (enqueue-only evaluation, the legacy behaviour).
 *
 * <p>The scheduler is owned by this dispatcher and shut down in {@link #clear()}
 * (invoked by {@link PersistenceRunner#stop()} when the actor stops), so it has
 * the same lifetime as the actor's persistence runner and leaks no threads.
 *
 * @param <E> the persistence event type
 */
public final class BackpressurePersistenceDispatcher<E> implements PersistenceDispatcher<E> {

    private final PersistenceDispatcher<E> delegate;
    private final int highWatermark;
    private final int lowWatermark;
    private final BackpressureCallback callback;

    private final AtomicBoolean underPressure = new AtomicBoolean(false);

    /** Periodic evaluator; {@code null} when {@code evaluateIntervalMs <= 0}. */
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> evaluatorTask;

    /**
     * Creates a dispatcher that evaluates watermarks <b>only on enqueue</b>
     * (no periodic evaluator). Suitable when upstream keeps enqueuing while the
     * buffer drains; otherwise prefer the {@code evaluateIntervalMs} constructor
     * so the falling edge (release) fires reliably.
     *
     * @param delegate      the dispatcher whose buffer depth is monitored
     * @param highWatermark buffer depth at which backpressure is signalled; must be &gt; {@code lowWatermark}
     * @param lowWatermark  buffer depth at which pressure is released; must be &gt;= 0
     * @param callback      receives the edge-triggered transitions
     */
    public BackpressurePersistenceDispatcher(PersistenceDispatcher<E> delegate,
                                             int highWatermark,
                                             int lowWatermark,
                                             BackpressureCallback callback) {
        this(delegate, highWatermark, lowWatermark, callback, 0L);
    }

    /**
     * Creates a dispatcher with an optional periodic watermark evaluator.
     *
     * @param delegate            the dispatcher whose buffer depth is monitored
     * @param highWatermark       buffer depth at which backpressure is signalled; must be &gt; {@code lowWatermark}
     * @param lowWatermark        buffer depth at which pressure is released; must be &gt;= 0
     * @param callback            receives the edge-triggered transitions
     * @param evaluateIntervalMs  how often (ms) to re-evaluate the buffer depth
     *                            in the background so the falling edge fires as
     *                            the buffer drains without new enqueues; {@code 0}
     *                            (or negative) disables the scheduler
     */
    public BackpressurePersistenceDispatcher(PersistenceDispatcher<E> delegate,
                                             int highWatermark,
                                             int lowWatermark,
                                             BackpressureCallback callback,
                                             long evaluateIntervalMs) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.callback = Objects.requireNonNull(callback, "callback must not be null");
        if (lowWatermark < 0)
            throw new IllegalArgumentException("lowWatermark must be >= 0, got " + lowWatermark);
        if (highWatermark <= lowWatermark)
            throw new IllegalArgumentException(
                    "highWatermark (%d) must be > lowWatermark (%d)".formatted(highWatermark, lowWatermark));
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;

        if (evaluateIntervalMs > 0) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "bufferfly-backpressure-evaluator");
                t.setDaemon(true);
                return t;
            });
            // Re-evaluate periodically; evaluate(...) is idempotent (guarded by
            // the AtomicBoolean), so a tick only fires a callback on an actual
            // edge crossing. Swallow exceptions so a transient hiccup never kills
            // the recurring task.
            this.evaluatorTask = scheduler.scheduleAtFixedRate(() -> {
                try {
                    evaluate(delegate.size());
                } catch (RuntimeException ignored) {
                    // never let the periodic evaluator die
                }
            }, evaluateIntervalMs, evaluateIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.evaluatorTask = null;
        }
    }

    @Override
    public void enqueue(PersistenceRunner.PersistenceEvent<E> event, PersistenceRunner<E> runner) {
        delegate.enqueue(event, runner);
        evaluate(delegate.size());
    }

    @Override
    public void clear() {
        delegate.clear();
        // Stop the periodic evaluator: the runner is being stopped, so this
        // dispatcher (and its scheduler thread) must not outlive it.
        if (evaluatorTask != null) {
            evaluatorTask.cancel(true);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
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
     *
     * <p>Invoked both from {@link #enqueue} (rising edge, immediately) and from
     * the periodic evaluator (falling edge, as the buffer drains). The
     * {@link AtomicBoolean} guard makes it safe to call from either thread and
     * guarantees each edge fires exactly once.
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
