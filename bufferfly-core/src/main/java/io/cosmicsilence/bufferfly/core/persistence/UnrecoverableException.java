package io.cosmicsilence.bufferfly.core.persistence;

/**
 * Thrown by {@link TransactionalOperations#run} to signal that the batch
 * failure is permanent and should not be retried.
 *
 * <p>{@link BatchingPersistenceRunner} will discard the batch and move on.
 * No items in the batch will be retried individually.
 *
 * <p>Use this for failures that are independent of the data content, such as
 * a schema migration in progress, a misconfigured connection pool, or an
 * external service that is permanently unavailable.
 */
public class UnrecoverableException extends PersistenceException {

    public UnrecoverableException(String message) {
        super(message);
    }

    public UnrecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableException(Throwable cause) {
        super(cause);
    }
}
