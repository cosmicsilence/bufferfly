package io.bufferfly.spring.resilience;

/**
 * A persistence event for the resilience suite: a simple string {@code body}
 * tagged with the partition it belongs to, an auto-incremented {@code id}
 * (assigned by the per-partition actor, unique within a partition), and the
 * Kafka {@code offset} it was consumed from (used both for the idempotent
 * insert and for advancing the fencing token).
 *
 * @param id        per-partition auto-increment id assigned by the actor
 * @param partition the Kafka partition this message belongs to
 * @param offset    the Kafka offset this message was consumed from
 * @param body      the string payload to persist
 */
public record Message(long id, int partition, long offset, String body) {
}
