package io.bufferfly.core.persistence;

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
 *   <li>{@link SingleItemException} — each element is re-submitted individually
 *       to isolate the poison pill:
 *       <ul>
 *         <li>Items that succeed are committed normally.</li>
 *         <li>The first item that throws a {@link PersistenceException} is
 *             identified as the poison pill —
 *             {@link TransactionalOperations#onPoisonPill} is called for it
 *             and processing continues with the next item.</li>
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

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    private final TransactionalOperations<E> operations;
    private final PersistenceDispatcher<E> dispatcher;
    private final PersistenceConfig<E> config;
    private final SideEffectProcessor sideEffectProcessor;

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

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

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
    }

    public PersistenceConfig<E> config() {
        return config;
    }

    // -----------------------------------------------------------------------
    // PersistenceRunner API
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Flush pipeline — called by NaturalBatchingDispatcher on its VT
    // -----------------------------------------------------------------------

    /**
     * Flushes one batch through the error-handling pipeline.
     * Called exclusively by the {@link PersistenceDispatcher} on a virtual thread.
     */
    @Override
    public void persistBatch(List<PersistenceEvent<E>> batch) {
        persistWithRetry(batch);
    }

    // -----------------------------------------------------------------------
    // Error-handling internals
    // -----------------------------------------------------------------------

    private void persistWithRetry(List<PersistenceEvent<E>> batch) {
        int attempts = 0;
        while (true) {
            try {
                operations.persistOnTransaction(List.copyOf(batch
                        .stream()
                        .map(PersistenceEvent::event)
                        .toList()));
                batch.forEach(e -> sideEffectProcessor.process(e.sideEffect()));
                return; // success
            } catch (SingleItemException e) {
                flushItemByItem(batch);
                handleUnrecoverable();
                return;
            } catch (TransientException e) {
                attempts++;
                if (attempts > config.maxRetries()) {
                    handleUnrecoverable();
                    return;
                }
                backOff(attempts);
            } catch (UnrecoverableException e) {
                handleUnrecoverable();
                return;
            } catch (Exception e) {
                // Unexpected exception → treat as unrecoverable
                handleUnrecoverable();
                return;
            }
        }
    }

    /**
     * Re-submits each element individually to isolate poison pills.
     *
     * <ul>
     *   <li>Success → committed, continue to next item.</li>
     *   <li>{@link PersistenceException} (including {@link SingleItemException})
     *       → the item is the poison pill; call
     *       {@link TransactionalOperations#onPoisonPill} and continue.</li>
     *   <li>{@link UnrecoverableException} or any unexpected exception on a
     *       single-item run → call {@link #handleUnrecoverable()} and abort
     *       the remainder of the batch.</li>
     * </ul>
     */
    private void flushItemByItem(List<PersistenceEvent<E>> batch) {
        for (PersistenceEvent<E> item : batch) {
            try {
                operations.persistOnTransaction(List.of(item.event()));
                sideEffectProcessor.process(item.sideEffect());
            } catch (UnrecoverableException e) {
                handleUnrecoverable();
                return; // abandon remainder of batch
            } catch (PersistenceException e) {
                // Poison pill — notify and continue with remaining items
                try {
                    operations.onPoisonPill(item.event(), e);

                } catch (Exception ignored) {
                    // onPoisonPill must not throw — swallow defensively
                }
            } catch (Exception e) {
                // Unexpected exception on a single item → unrecoverable
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
        try {
            onUnrecoverable.run();
        } catch (Exception ignored) {
            // The hook itself must not throw — swallow defensively.
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
            Thread.currentThread().interrupt();
        }
    }

    private enum State {
        STARTED,
        PAUSED,
        STOPPED
    }
}
