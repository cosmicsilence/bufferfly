package io.bufferfly.spring.rewind;

/**
 * A persistence event for the two-partition mid-batch restart/rewind test: a
 * string {@code body} tagged with its partition and the Kafka {@code offset} it
 * was consumed from (used for the idempotent insert and for advancing the
 * fencing token).
 *
 * @param partition the Kafka partition this record belongs to
 * @param offset    the Kafka offset this record was consumed from
 * @param body      the string payload to persist
 */
public record RewindMessage(int partition, long offset, String body) {
}
