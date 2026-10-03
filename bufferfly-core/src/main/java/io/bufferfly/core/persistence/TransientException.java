package io.bufferfly.core.persistence;

/**
 * Thrown by {@link TransactionalOperations#run} to signal a temporary failure
 * that is expected to succeed on a subsequent attempt with the same batch.
 *
 * <p>{@link BatchingPersistenceRunner} will retry the entire batch up to
 * {@link BatchingPersistenceRunner.Config#maxRetries()} times with the
 * configured back-off before giving up and escalating to
 * {@link UnrecoverableException} handling.
 *
 * <p>Use this for transient infrastructure failures such as a momentary
 * network timeout, a deadlock that rolled back the transaction, or a
 * connection pool exhaustion spike.
 */
public class TransientException extends PersistenceException {

    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransientException(Throwable cause) {
        super(cause);
    }
}
