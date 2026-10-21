package io.bufferfly.core.backpressure;

/**
 * Thrown by a backpressure-aware dispatcher operating under the
 * {@link BackpressureStrategy#THROW} strategy when a {@code dispatch} is
 * attempted while the system is under pressure.
 *
 * <p>This is an unchecked, fail-fast signal to the upstream producer (for
 * example a Kafka listener) that the in-memory shock absorber is saturated and
 * the caller should stop producing — typically by pausing the partition and
 * letting it be replayed once pressure is relieved.
 */
public class UnderPressureException extends RuntimeException {

    public UnderPressureException(String message) {
        super(message);
    }

    public UnderPressureException(String message, Throwable cause) {
        super(message, cause);
    }
}
