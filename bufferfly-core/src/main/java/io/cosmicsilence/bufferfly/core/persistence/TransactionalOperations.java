package io.cosmicsilence.bufferfly.core.persistence;

import java.util.List;

/**
 * User-implemented contract for writing a batch of persistence events to an
 * underlying data store.
 *
 * <p>Implementations are called by {@link BatchingPersistenceRunner} on a
 * dedicated virtual thread. Each {@link #run} call wraps one natural batch and
 * should be treated as a single atomic unit of work (e.g. one database
 * transaction, one bulk-insert, or one array-update statement).
 *
 * <h2>Error signalling</h2>
 * Throw the appropriate {@link PersistenceException} subclass to tell the
 * runner how to react:
 * <ul>
 *   <li>{@link UnrecoverableException} — discard the batch and move on.</li>
 *   <li>{@link TransientException} — retry the entire batch.</li>
 *   <li>{@link SingleItemException} — the batch contains a poison pill; the
 *       runner will re-submit each item individually and call
 *       {@link #onPoisonPill} for the offending element.</li>
 * </ul>
 * Any other unchecked exception is treated as {@link UnrecoverableException}.
 *
 * @param <E> the persistence event type
 */
public interface TransactionalOperations<E> {

    /**
     * Persists {@code batch} atomically.
     *
     * @param batch a non-empty, immutable snapshot of events to persist
     * @throws PersistenceException to signal how the runner should handle a failure
     */
    void run(List<E> batch);

    /**
     * Called when an individual element in a batch is identified as a poison
     * pill — an item that cannot be persisted regardless of retries (e.g. a
     * duplicate primary key, a null constraint violation).
     *
     * <p>This method must not throw. Implementations should log, dead-letter,
     * or metric-alert as appropriate for the use case.
     *
     * @param event     the poison-pill event
     * @param cause     the exception thrown when attempting to persist it alone
     */
    void onPoisonPill(E event, Exception cause);
}
