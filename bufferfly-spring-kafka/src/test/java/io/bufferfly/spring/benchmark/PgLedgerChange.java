package io.bufferfly.spring.benchmark;

import io.bufferfly.spring.persistence.OffsetBearing;

/**
 * One persistence event for the benchmark ledger: an account change plus its
 * Kafka coordinates. Implements {@link OffsetBearing} so it can flow through
 * the main-source {@code AbstractJdbcTransactionalOperations} fencing machinery
 * in the actor approach.
 */
public record PgLedgerChange(
        String topic,
        int partition,
        long offset,
        String accountNumber,
        long amount,
        long resultingBalance) implements OffsetBearing {
}
