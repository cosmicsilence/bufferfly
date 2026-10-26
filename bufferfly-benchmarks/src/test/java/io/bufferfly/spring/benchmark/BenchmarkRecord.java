package io.bufferfly.spring.benchmark;

import io.bufferfly.spring.ledger.AddToAccount;

/**
 * The unit of work fed to a {@link PgLedgerActor}: an {@link AddToAccount}
 * command paired with the Kafka coordinates it was consumed from. A plain
 * analog of {@code ConsumedRecord} so the benchmark can drive the actor runner
 * directly from a bare {@code KafkaConsumer} without the Spring container.
 */
public record BenchmarkRecord(String topic, int partition, long offset, AddToAccount value) {
}
