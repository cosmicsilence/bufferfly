package io.bufferfly.core.persistence;

/**
 * Base class for all persistence-layer exceptions thrown by
 * {@link TransactionalOperations} implementations.
 *
 * <p>Subclass with the appropriate type to signal how
 * {@link BatchingPersistenceRunner} should react to the failure:
 * <ul>
 *   <li>{@link UnrecoverableException} — discard the batch, do not retry.</li>
 *   <li>{@link TransientException} — retry the entire batch.</li>
 *   <li>{@link SingleItemException} — re-run items one-by-one to find the
 *       poison pill.</li>
 * </ul>
 */
public class PersistenceException extends RuntimeException {

    public PersistenceException(String message) {
        super(message);
    }

    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistenceException(Throwable cause) {
        super(cause);
    }
}
