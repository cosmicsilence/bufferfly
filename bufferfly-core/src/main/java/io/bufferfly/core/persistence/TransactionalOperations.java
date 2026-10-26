package io.bufferfly.core.persistence;

import java.util.List;

/**
 * User-implemented contract for writing a batch of persistence events to an
 * underlying data store.
 *
 * <p>Implementations are called by {@link BatchingPersistenceRunner} on a
 * dedicated virtual thread. Each {@link #persistOnTransaction} call wraps one natural batch and
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
 *       runner re-submits each item individually to find it, dead-letters the
 *       offending element via {@link #onPoisonPill}, and then <b>halts</b> (the
 *       items after the pill are not persisted and are re-processed when the
 *       actor restarts and replays from the committed offset).</li>
 * </ul>
 * Any other unchecked exception is treated as {@link UnrecoverableException}.
 *
 * @param <E> the persistence event type
 */
public interface TransactionalOperations<E> {

    /**
     * Persists {@code batch} atomically.
     *
     * <p>Kept for implementations that do not participate in the fencing-token
     * protocol (e.g. stores without a per-partition fencing token). Token-backed
     * implementations should override {@link #persistOnTransaction(List, long)}
     * instead; the default of this method is unused once that overload is
     * implemented.
     *
     * @param batch a non-empty, immutable snapshot of events to persist
     * @throws PersistenceException to signal how the runner should handle a failure
     */
    default void persistOnTransaction(List<E> batch) {
        throw new UnsupportedOperationException(
                "persistOnTransaction(batch) is not implemented; implement "
                        + "persistOnTransaction(batch, currentToken) or override this method");
    }

    /**
     * Persists {@code batch} atomically and advances the per-partition
     * optimistic-locking <b>fencing token</b> from the authoritative
     * {@code currentToken} the caller holds to the batch's new high-water value,
     * <em>in the same transaction</em>.
     *
     * <p>The token is any monotonically increasing number the storage layer can
     * compare-and-set on — a Kafka offset, a WAL position, a DB sequence. The
     * core engine never interprets it; it only carries it through so the store
     * can fence zombie writers. (In the Kafka adapter the token <em>is</em> the
     * consumed offset.)
     *
     * <p>{@code currentToken} is the token this writer believes it holds: the
     * value the owning actor loaded from the store when it (re)started, then
     * advanced by this runner after each successful batch. The implementation
     * MUST advance the stored token with a compare-and-set guarded by this exact
     * value ({@code WHERE token = currentToken}) and MUST NOT re-read the stored
     * token to decide what to write — a stale read is precisely the race this
     * protocol removes. If the CAS matches zero rows a zombie actor advanced the
     * token concurrently; throw {@link UnrecoverableException} so the transaction
     * rolls back and the actor restarts clean.
     *
     * <p>The returned value becomes the caller's {@code currentToken} for the
     * next batch. On success this is the highest token just committed; an empty
     * batch returns {@code currentToken} unchanged.
     *
     * <p>The default implementation delegates to {@link #persistOnTransaction(List)}
     * and returns {@code currentToken} unchanged, for stores that do not fence.
     *
     * @param batch        a non-empty, immutable snapshot of events to persist
     * @param currentToken the authoritative fencing token this writer holds
     *                     ({@code < 0} when nothing has been committed yet)
     * @return the new authoritative fencing token to use for the next batch
     * @throws PersistenceException to signal how the runner should handle a failure
     */
    default long persistOnTransaction(List<E> batch, long currentToken) {
        persistOnTransaction(batch);
        return currentToken;
    }

    /**
     * Called when an individual element in a batch is identified as a poison
     * pill — an item that cannot be persisted regardless of retries (e.g. a
     * duplicate primary key, a null constraint violation).
     *
     * <p>This method must not throw. Implementations should log, dead-letter,
     * or metric-alert as appropriate for the use case.
     *
     * <p>{@code currentToken} is the authoritative fencing token the runner
     * holds at the point the pill was isolated — the value advanced by the good
     * items persisted before it in the same item-by-item pass. A token-backed
     * implementation that steps the stored token past the dead-lettered record
     * MUST compare-and-set from this exact value (never re-read it), for the same
     * reason the batch path does: a re-read reopens the zombie race.
     *
     * <p>Returns the fencing token in force <em>after</em> dead-lettering: for a
     * token-backed store that stepped the token past the pill, that is the pill's
     * token; the runner adopts it so the next isolated item fences from the right
     * value instead of a stale one. A store that does not advance any token
     * returns {@code currentToken} unchanged.
     *
     * @param event        the poison-pill event
     * @param currentToken the authoritative fencing token held when the pill was
     *                     isolated ({@code < 0} if nothing has been committed yet)
     * @param cause        the exception thrown when attempting to persist it alone
     * @return the fencing token to use for the next item (the advanced token, or
     *         {@code currentToken} if the store advances nothing)
     */
    long onPoisonPill(E event, long currentToken, Exception cause);
}
