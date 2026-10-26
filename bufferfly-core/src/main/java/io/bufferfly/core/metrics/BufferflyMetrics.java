package io.bufferfly.core.metrics;

import java.util.function.IntSupplier;

/**
 * The BufferFly observability <b>SPI</b>: a single, dependency-free facade the
 * engine calls to report what its hot paths are doing. It has <b>no runtime
 * dependency on any metrics library</b> — the core only ever talks to this
 * interface. Backends (OpenTelemetry, Micrometer, a test double, …) live in
 * separate modules that implement it.
 *
 * <h2>Design goals</h2>
 * <ul>
 *   <li><b>Non-intrusive.</b> Every method is a cheap, fire-and-forget event
 *       notification. The default is {@link #NOOP}, so a BufferFly instance with
 *       no collector wired behaves exactly as before — no allocation, no branch
 *       beyond a single virtual call the JIT can inline and prune.</li>
 *   <li><b>Counters are push; gauges are pull.</b> Discrete events
 *       (an enqueue, a refusal, a poison pill) are reported as they happen via
 *       {@code record*}/{@code increment*} methods. Continuously-varying depths
 *       (a mailbox size, a persistence buffer depth) are <em>registered once</em>
 *       as an {@link IntSupplier} the backend samples on its own schedule — the
 *       engine never pushes a gauge value on the hot path.</li>
 *   <li><b>Labelled by component.</b> Mailbox/buffer metrics carry the actor or
 *       runner {@code name} so a backend can emit per-component time series.</li>
 * </ul>
 *
 * <h2>What is observed</h2>
 * <ul>
 *   <li><b>Mailbox / dispatch</b> — {@link #mailboxEnqueued}, {@link #mailboxRefused},
 *       and a registered {@link #registerMailboxSize mailbox-size gauge}.</li>
 *   <li><b>Persistence runner</b> — {@link #batchPersisted}, {@link #poisonPill},
 *       {@link #unrecoverableFailure}, {@link #batchRetry}.</li>
 *   <li><b>Persistence dispatcher</b> — a registered
 *       {@link #registerPersistenceBufferDepth buffer-depth gauge}.</li>
 *   <li><b>Backpressure</b> — {@link #backpressureEngaged}, {@link #backpressureReleased}.</li>
 * </ul>
 *
 * <p>Implementations MUST be thread-safe and MUST NOT throw: a metrics backend
 * hiccup must never perturb the engine. The engine still guards calls
 * defensively, but a well-behaved implementation swallows its own errors.
 */
public interface BufferflyMetrics {

    // -----------------------------------------------------------------------
    // Mailbox / dispatch
    // -----------------------------------------------------------------------

    /**
     * A message was accepted into {@code actorName}'s mailbox.
     *
     * @param actorName the owning actor's name
     */
    void mailboxEnqueued(String actorName);

    /**
     * A message was <b>refused</b> by {@code actorName}'s mailbox (the bounded
     * mailbox was full) — i.e. a {@code MailBoxRefusedException} is about to be
     * thrown. This is the signal that backpressure is overdue or that the actor
     * cannot keep up.
     *
     * @param actorName the owning actor's name
     */
    void mailboxRefused(String actorName);

    /**
     * Registers a gauge that reports {@code actorName}'s current mailbox depth.
     * Called once, when the mailbox is created; the backend samples
     * {@code sizeSupplier} on its own schedule. A no-op backend ignores it.
     *
     * @param actorName    the owning actor's name
     * @param sizeSupplier supplies the instantaneous mailbox depth; cheap, O(1)
     */
    void registerMailboxSize(String actorName, IntSupplier sizeSupplier);

    // -----------------------------------------------------------------------
    // Persistence runner
    // -----------------------------------------------------------------------

    /**
     * A batch of {@code size} events was durably persisted (one bulk transaction)
     * by the persistence runner owned by {@code component}.
     *
     * @param component the owning component (typically the actor name)
     * @param size      the number of events in the committed batch
     */
    void batchPersisted(String component, int size);

    /**
     * A poison pill was isolated and dead-lettered by {@code component}'s runner
     * (the runner halts afterwards).
     *
     * @param component the owning component (typically the actor name)
     */
    void poisonPill(String component);

    /**
     * An unrecoverable persistence failure on {@code component}'s runner fired the
     * {@code onUnrecoverable} hook (a fatal exception, a failed fence, or
     * exhausted transient retries).
     *
     * @param component the owning component (typically the actor name)
     */
    void unrecoverableFailure(String component);

    /**
     * A batch persist attempt on {@code component}'s runner failed transiently and
     * is being retried with back-off. Recorded once per retry attempt.
     *
     * @param component the owning component (typically the actor name)
     */
    void batchRetry(String component);

    /**
     * Registers a gauge that reports the current buffer depth of a persistence
     * dispatcher owned by {@code component}. Called once, when the dispatcher is
     * wired; the backend samples {@code depthSupplier} on its own schedule.
     *
     * @param component     the owning component (typically the actor name)
     * @param depthSupplier supplies the instantaneous buffer depth; cheap, O(1)
     */
    void registerPersistenceBufferDepth(String component, IntSupplier depthSupplier);

    // -----------------------------------------------------------------------
    // Backpressure
    // -----------------------------------------------------------------------

    /**
     * Backpressure was <b>engaged</b> on {@code component} (rising edge: the buffer
     * reached the high watermark). Fires once per engage transition.
     *
     * @param component the owning component (typically the actor name)
     */
    void backpressureEngaged(String component);

    /**
     * Backpressure was <b>released</b> on {@code component} (falling edge: the buffer
     * receded to the low watermark). Fires once per release transition.
     *
     * @param component the owning component (typically the actor name)
     */
    void backpressureReleased(String component);

    // -----------------------------------------------------------------------
    // Kafka partition lifecycle (bufferfly-spring-kafka)
    // -----------------------------------------------------------------------

    /**
     * A Kafka partition was <b>paused</b> for {@code component} — fetching stops
     * for this partition. Driven either proactively by the persistence buffer
     * crossing the high watermark, or reactively when a record is rejected under
     * pressure.
     *
     * @param component the owning component (the per-partition actor name)
     */
    void partitionPaused(String component);

    /**
     * A Kafka partition was <b>resumed</b> for {@code component} — fetching
     * restarts after the buffer drained back to the low watermark.
     *
     * @param component the owning component (the per-partition actor name)
     */
    void partitionResumed(String component);

    /**
     * The actor for {@code component} was <b>restarted</b> after an unrecoverable
     * failure (the supervisor stopped it, rewound the offset, and started a fresh
     * one). A climbing count here is a crash loop.
     *
     * @param component the owning component (the per-partition actor name)
     */
    void actorRestarted(String component);

    /**
     * A consumed record was <b>rejected</b> (left un-acked) and will be
     * redelivered — because the actor pushed back under pressure, or because no
     * active actor currently owns the partition (e.g. mid-restart), or a stale
     * pre-rewind record was discarded.
     *
     * @param component the owning component (the per-partition actor name)
     */
    void recordRejected(String component);

    /**
     * A Kafka partition was <b>assigned</b> to this instance (a rebalance brought
     * up a fresh actor for it).
     *
     * @param component the owning component (the per-partition actor name)
     */
    void partitionAssigned(String component);

    /**
     * A Kafka partition was <b>revoked</b> from this instance (a rebalance tore
     * down its actor).
     *
     * @param component the owning component (the per-partition actor name)
     */
    void partitionRevoked(String component);

    // -----------------------------------------------------------------------
    // No-op default
    // -----------------------------------------------------------------------

    /**
     * The default collector: does nothing. Used everywhere a collector is not
     * explicitly wired, so observability is strictly opt-in and zero-cost when
     * off.
     */
    BufferflyMetrics NOOP = new BufferflyMetrics() {
        @Override public void mailboxEnqueued(String actorName) { }
        @Override public void mailboxRefused(String actorName) { }
        @Override public void registerMailboxSize(String actorName, IntSupplier sizeSupplier) { }
        @Override public void batchPersisted(String component, int size) { }
        @Override public void poisonPill(String component) { }
        @Override public void unrecoverableFailure(String component) { }
        @Override public void batchRetry(String component) { }
        @Override public void registerPersistenceBufferDepth(String component, IntSupplier depthSupplier) { }
        @Override public void backpressureEngaged(String component) { }
        @Override public void backpressureReleased(String component) { }
        @Override public void partitionPaused(String component) { }
        @Override public void partitionResumed(String component) { }
        @Override public void actorRestarted(String component) { }
        @Override public void recordRejected(String component) { }
        @Override public void partitionAssigned(String component) { }
        @Override public void partitionRevoked(String component) { }

        @Override public String toString() { return "BufferflyMetrics.NOOP"; }
    };
}
