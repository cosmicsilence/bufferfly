package io.bufferfly.core.persistence;

/**
 * Thrown by {@link TransactionalOperations#run} to signal that the batch
 * contains at least one poison-pill element — an item that will always cause
 * a constraint violation or data error regardless of retries (e.g. a duplicate
 * primary key, a referential integrity violation, or a value that exceeds a
 * column constraint).
 *
 * <p>{@link BatchingPersistenceRunner} reacts by re-submitting each item in
 * the failed batch individually to {@link TransactionalOperations#run} as a
 * single-element list. The first element that causes another exception is the
 * poison pill; {@link TransactionalOperations#onPoisonPill(Object, Exception)}
 * is called for it, and processing continues with the remaining elements.
 * Elements that succeed individually are committed normally.
 */
public class SingleItemException extends PersistenceException {

    public SingleItemException(String message) {
        super(message);
    }

    public SingleItemException(String message, Throwable cause) {
        super(message, cause);
    }

    public SingleItemException(Throwable cause) {
        super(cause);
    }
}
