package io.bufferfly.core.persistence;

import io.bufferfly.core.metrics.BufferflyMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link PersistenceRunner} that delegates scheduling and buffering to a
 * {@link PersistenceDispatcher} and focuses purely on batch error handling.
 *
 * <p>This class owns <strong>no threads</strong>. Virtual thread scheduling
 * is entirely the responsibility of the injected {@link PersistenceDispatcher}
 * (typically {@link NaturalBatchingDispatcher}).
 *
 * <h2>Error handling on flush</h2>
 * <ul>
 *   <li>{@link UnrecoverableException} — the {@code onUnrecoverable} hook is
 *       invoked. The default action (and the intended production action) is to
 *       halt the owning actor and let the Kafka consumer restart from the last
 *       committed offset, replaying messages from a known-good state.</li>
 *   <li>{@link TransientException} — entire batch is retried up to
 *       {@link PersistenceConfig#maxRetries} times with exponential back-off. After all
 *       retries are exhausted {@code onUnrecoverable} is invoked.</li>
 *   <li>{@link SingleItemException} — the batch contains a poison pill, so the
 *       runner re-submits elements individually to find it:
 *       <ul>
 *         <li>Items <em>before</em> the pill that succeed are committed normally.</li>
 *         <li>The first item that throws a {@link PersistenceException} is the
 *             poison pill — {@link TransactionalOperations#onPoisonPill} is called
 *             to dead-letter it (advancing the fence past it), and the runner then
 *             <b>halts</b>: items <em>after</em> the pill are deliberately left
 *             un-persisted and {@code onUnrecoverable} is invoked. They are
 *             re-processed when the restarted actor replays from the committed
 *             offset, so the durable store never gets ahead of a record the store
 *             rejected.</li>
 *         <li>If a single-item run throws {@link UnrecoverableException} or
 *             any unexpected exception, {@code onUnrecoverable} is invoked
 *             and the remainder of the batch is abandoned.</li>
 *       </ul>
 *   </li>
 *   <li>Any other unchecked exception — treated as {@link UnrecoverableException}.</li>
 * </ul>
 *
 * <h2>onUnrecoverable hook</h2>
 * Injected at construction time as a {@link Runnable}. The framework wires
 * this to halt the owning actor reference and signal the Kafka listener to
 * seek back to the last committed offset, effectively restarting clean.
 * A no-op default is provided for tests and simple use-cases.
 *
 * @param <E> the persistence event type
 */
public final class BatchingPersistenceRunner<E> implements PersistenceRunner<E> {

    private static final Logger log = LoggerFactory.getLogger(BatchingPersistenceRunner.class);

    private final TransactionalOperations<E> operations;
    private final PersistenceDispatcher<E> dispatcher;
    private final PersistenceConfig<E> config;
    private final SideEffectProcessor sideEffectProcessor;

    /**
     * Observability sink taken from {@link PersistenceConfig#metrics()}. Defaults
     * to {@link BufferflyMetrics#NOOP}. Every call is fire-and-forget on the
     * persistence thread.
     */
    private final BufferflyMetrics metrics;

    /** Metrics component label (the owning actor's name); never null. */
    private final String component;

    /**
     * Called when a batch failure is unrecoverable (after retries are
     * exhausted, or immediately for {@link UnrecoverableException}).
     *
     * <p>The intended production implementation halts the owning actor and
     * triggers a Kafka seek-to-committed-offset so the partition is replayed
     * from a clean state. Defaults to a no-op.
     */
    private final Runnable onUnrecoverable;

    private volatile State state = State.STARTED;

    /**
     * The authoritative fencing token this runner currently holds. Seeded by the
     * owning actor on restart via {@link #initToken(long)} (from the token
     * durably stored for the partition), then advanced to the value returned by
     * {@link TransactionalOperations#persistOnTransaction(List, long)} after
     * every successful commit. Passing this known value down (rather than having
     * the storage layer re-read it) is what makes the fence a true optimistic
     * lock: a zombie actor holds a stale token and its compare-and-set fails.
     *
     * <p>The token is any monotonically increasing number the storage layer can
     * compare-and-set on (a Kafka offset, a WAL position, a sequence); the core
     * engine treats it only as an opaque fencing token.
     *
     * <p>Mutated only on the single persistence thread inside {@link #persistBatch}
     * and seeded once from the actor thread in {@code preStart}; {@code volatile}
     * guarantees the seed is visible to the persistence thread.
     */
    private volatile long currentToken = UNINITIALISED_TOKEN;

    /** Sentinel for "no token committed yet" (first batch ever for the partition). */
    public static final long UNINITIALISED_TOKEN = -1L;


    public BatchingPersistenceRunner(TransactionalOperations<E> operations) {
        this(new PersistenceConfig<>(operations));
    }

    public BatchingPersistenceRunner(PersistenceConfig<E> config) {
        this(config, (sf) -> {
        });
    }

    public BatchingPersistenceRunner(PersistenceConfig<E> config, SideEffectProcessor processor) {
        this(
                config.operations(),
                config,
                config.onUnrecoverable(),
                config.dispatcher() != null
                        ? config.dispatcher()
                        : new NaturalBatchingDispatcher<>(config.batchSize(), config.timeoutMs()),
                processor
        );
    }

    public BatchingPersistenceRunner(
            TransactionalOperations<E> operations,
            PersistenceConfig<E> config,
            SideEffectProcessor processor) {
        this(operations, config, config.onUnrecoverable(), processor);
    }

    /**
     * Constructor with a custom {@code onUnrecoverable} hook.
     * The hook is executed on the persistence virtual thread when an
     * unrecoverable failure occurs.
     */
    public BatchingPersistenceRunner(
            TransactionalOperations<E> operations,
            PersistenceConfig<E> config,
            Runnable onUnrecoverable,
            SideEffectProcessor processor) {
        this(operations, config, onUnrecoverable,
                config.dispatcher() != null
                        ? config.dispatcher()
                        : new NaturalBatchingDispatcher<>(config.batchSize(), config.timeoutMs()),
                processor);
    }

    /**
     * Full constructor — inject a custom {@link PersistenceDispatcher} (useful
     * for testing or alternative scheduling strategies).
     */
    public BatchingPersistenceRunner(
            TransactionalOperations<E> operations,
            PersistenceConfig<E> config,
            Runnable onUnrecoverable,
            PersistenceDispatcher<E> dispatcher,
            SideEffectProcessor processor) {
        if (operations == null) throw new NullPointerException("operations must not be null");
        if (config == null) throw new NullPointerException("config must not be null");
        if (onUnrecoverable == null) throw new NullPointerException("onUnrecoverable must not be null");
        if (dispatcher == null) throw new NullPointerException("dispatcher must not be null");
        if (processor == null) throw new NullPointerException("processor must not be null");
        this.operations = operations;
        this.config = config;
        this.onUnrecoverable = onUnrecoverable;
        this.dispatcher = dispatcher;
        this.sideEffectProcessor = processor;
        this.metrics = config.metrics() == null ? BufferflyMetrics.NOOP : config.metrics();
        this.component = config.componentName();
    }

    public PersistenceConfig<E> config() {
        return config;
    }

    /**
     * Seeds the authoritative fencing token this runner holds. Called by the
     * owning actor on (re)start — typically from {@code preStart} — with the
     * token durably stored for its partition (or {@link #UNINITIALISED_TOKEN}
     * when nothing has been committed yet). Must be called before any event is
     * enqueued; from that point the runner advances the token itself after each
     * committed batch.
     *
     * @param storedToken the last durably committed token for the partition,
     *                    or {@link #UNINITIALISED_TOKEN} if none
     */
    public void initToken(long storedToken) {
        this.currentToken = storedToken;
    }

    /** @return the fencing token this runner currently holds. */
    public long currentToken() {
        return currentToken;
    }

    /**
     * Enqueues {@code event} and signals the dispatcher to schedule a consume
     * loop if one is not already running. Never blocks.
     *
     * @throws IllegalStateException if {@link #stop()} has already been called
     */
    @Override
    public void enqueue(PersistenceEvent<E> event) {
        if (state == State.STOPPED) {
            throw new IllegalStateException(
                    "BatchingPersistenceRunner is stopped; cannot enqueue new events");
        }
        if (event == null) throw new NullPointerException("event must not be null");
        dispatcher.enqueue(event, this);
    }

    /**
     * Stops accepting new events and instructs the dispatcher to cancel its
     * in-flight task and discard any buffered events.
     */
    @Override
    public void stop() {
        state = State.STOPPED;
        dispatcher.clear();
    }

    /**
     * Flushes one batch through the error-handling pipeline.
     * Called exclusively by the {@link PersistenceDispatcher} on a virtual thread.
     *
     * <p>No-op once the runner is {@link State#STOPPED} — whether stopped
     * explicitly via {@link #stop()} or halted by an unrecoverable failure
     * (see {@link #handleUnrecoverable()}). This guarantees that once the actor
     * is being restarted no further batches are processed by this (now defunct)
     * runner instance, preventing duplicate work racing the fresh instance.
     */
    @Override
    public void persistBatch(List<PersistenceEvent<E>> batch) {
        if (state == State.STOPPED) {
            return;
        }
        persistWithRetry(batch);
    }

    private void persistWithRetry(List<PersistenceEvent<E>> batch) {
        int attempts = 0;
        while (true) {
            try {
                long newToken = operations.persistOnTransaction(
                        List.copyOf(batch
                                .stream()
                                .map(PersistenceEvent::event)
                                .toList()),
                        currentToken);
                // Adopt the advanced fencing token for the next batch. The CAS in
                // the operations layer succeeded against our known currentToken,
                // so we now own the token at its new value.
                currentToken = newToken;
                batch.forEach(e -> e.sideEffects().forEach(sideEffectProcessor::process));
                metrics.batchPersisted(component, batch.size());
                return; // success
            } catch (SingleItemException e) {
                // Poison pill in the batch: isolate item-by-item to FIND the pill.
                // Good items before it persist; the pill is dead-lettered via
                // onPoisonPill (fence advanced past it); then we HALT — the items
                // after the pill are left un-persisted. The unrecoverable hook
                // (e.g. in a sharded Kafka setup) restarts the actor, which resumes
                // from the last durably committed offset and re-processes the
                // abandoned tail. This runner instance stops here.
                log.warn("Batch of {} flagged a poison pill ({}); isolating item-by-item to find and halt at it",
                        batch.size(), e.getMessage());
                flushItemByItem(batch);
                handleUnrecoverable();
                return;
            } catch (TransientException e) {
                attempts++;
                if (attempts > config.maxRetries()) {
                    log.error("Transient failure persisting batch of {} not resolved after {} retries; treating as unrecoverable",
                            batch.size(), config.maxRetries(), e);
                    handleUnrecoverable();
                    return;
                }
                log.warn("Transient failure persisting batch of {} (attempt {}/{}): {}; retrying with back-off",
                        batch.size(), attempts, config.maxRetries(), e.getMessage());
                metrics.batchRetry(component);
                backOff(attempts);
            } catch (UnrecoverableException e) {
                log.error("Unrecoverable failure persisting batch of {}; halting runner and invoking recovery hook",
                        batch.size(), e);
                handleUnrecoverable();
                return;
            } catch (Exception e) {
                // Unexpected exception → treat as unrecoverable
                log.error("Unexpected {} persisting batch of {}; treating as unrecoverable",
                        e.getClass().getSimpleName(), batch.size(), e);
                handleUnrecoverable();
                return;
            }
        }
    }

    /**
     * Re-submits each element individually to find and isolate the poison pill,
     * then <b>halts</b> — it does <em>not</em> drain the rest of the batch.
     *
     * <ul>
     *   <li>Success → committed, continue to the next item.</li>
     *   <li>{@link PersistenceException} (including {@link SingleItemException})
     *       → this item is the poison pill. {@link TransactionalOperations#onPoisonPill}
     *       is called to dead-letter it (a token-backed store advances the fence
     *       <em>past</em> it so a restart resumes <em>after</em> the pill, never
     *       re-delivering it), and then processing <b>stops</b>: the items that
     *       followed the pill in this batch are deliberately left un-persisted
     *       and the method returns so the caller can halt the runner.</li>
     *   <li>{@link UnrecoverableException} or any unexpected exception on a
     *       single-item run → halt immediately and abandon the remainder.</li>
     * </ul>
     *
     * <h2>Why halt at the pill instead of persisting the items after it</h2>
     * The actor already applied every event in this batch to its in-memory state
     * (that is what produced them), but persistence stops at the pill. If we kept
     * persisting the post-pill items here, the durable store would contain writes
     * the actor made <em>after</em> a record it never durably accepted, while the
     * actor's own in-memory state reflects the whole batch — a split-brain that
     * surfaces on the next rehydration (the restarted actor replays from the
     * committed offset and re-derives a state that disagrees with what was
     * written). Halting makes the boundary clean: everything up to and including
     * the dead-lettered pill is durable and fenced; everything after it is
     * discarded here and re-processed deterministically when the actor restarts
     * and seeks back to the committed offset.
     */
    private void flushItemByItem(List<PersistenceEvent<E>> batch) {
        for (PersistenceEvent<E> item : batch) {
            try {
                // Persist this single item, fencing against the token we hold, and
                // adopt the advanced token so the next item's CAS matches.
                currentToken = operations.persistOnTransaction(List.of(item.event()), currentToken);
                item.sideEffects().forEach(sideEffectProcessor::process);
                metrics.batchPersisted(component, 1);
            } catch (UnrecoverableException e) {
                log.error("Unrecoverable failure while isolating batch item-by-item; abandoning remaining items", e);
                handleUnrecoverable();
                return; // abandon remainder of batch
            } catch (PersistenceException e) {
                // Poison pill found. Dead-letter it, then HALT: do not persist the
                // items after it in this batch. They were accepted into the actor's
                // in-memory state but must not be written past a record the store
                // rejected — the restarted actor will re-process them from the
                // committed offset, keeping persistence consistent with replay.
                log.warn("Poison pill detected ({}); dead-lettering via onPoisonPill and halting — "
                                + "the rest of the batch is abandoned and will be re-processed on restart",
                        e.getMessage());
                try {
                    // Hand the operations layer the authoritative token we hold
                    // right now (advanced by the good items isolated before this
                    // pill) and adopt the token it returns — the store stepped the
                    // fence past the dead-lettered record, so a restart's rewind
                    // resumes AFTER the pill rather than re-delivering it.
                    currentToken = operations.onPoisonPill(item.event(), currentToken, e);

                } catch (Exception swallowed) {
                    // onPoisonPill must not throw — swallow defensively
                    log.error("onPoisonPill threw while dead-lettering a poison pill; swallowing to keep the runner alive",
                            swallowed);
                }
                metrics.poisonPill(component);
                // HALT: the pill is dead-lettered and the fence is advanced past
                // it; stop here and let the caller invoke the unrecoverable hook.
                // Items after the pill are intentionally not persisted.
                return;
            } catch (Exception e) {
                // Unexpected exception on a single item → unrecoverable
                log.error("Unexpected {} while isolating batch item-by-item; treating as unrecoverable",
                        e.getClass().getSimpleName(), e);
                handleUnrecoverable();
                return; // abandon remainder of batch
            }
        }
    }

    /**
     * Invokes the {@code onUnrecoverable} hook provided at construction.
     * In production this halts the owning actor and triggers a Kafka
     * seek-to-committed-offset so the partition replays from a clean state.
     */
    private void handleUnrecoverable() {
        // Halt first: no further batches will be processed by this runner. The
        // hook typically restarts the actor, which brings up a fresh runner;
        // this defunct one must not keep draining batches and racing it.
        state = State.STOPPED;
        metrics.unrecoverableFailure(component);
        try {
            onUnrecoverable.run();
        } catch (Exception swallowed) {
            // The hook itself must not throw — swallow defensively.
            log.error("onUnrecoverable hook threw; swallowing so the halt completes cleanly", swallowed);
        }
    }

    /**
     * Exponential back-off between transient retries.
     * Delay = 50 × 2^(attempt−1) ms, capped at 5 seconds.
     */
    private void backOff(int attempt) {
        long delayMs = Math.min(50L * (1L << (attempt - 1)), 5_000L);
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            log.warn("Interrupted during {} ms transient-retry back-off; re-setting interrupt flag", delayMs);
            Thread.currentThread().interrupt();
        }
    }

    private enum State {
        STARTED,
        PAUSED,
        STOPPED
    }
}
