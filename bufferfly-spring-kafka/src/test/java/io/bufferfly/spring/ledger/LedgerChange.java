package io.bufferfly.spring.ledger;

/**
 * One persistence event produced by the {@code LedgerActor} for each processed
 * message. It carries the account change <em>and</em> the Kafka offset that
 * produced it, so the persistence layer can both:
 * <ul>
 *   <li>insert the change (append-only ledger) and update the running balance, and</li>
 *   <li>advance the stored consumer offset — using the highest offset in the
 *       batch as an optimistic-locking fencing token.</li>
 * </ul>
 *
 * @param topic            source topic
 * @param partition        source partition
 * @param offset           source offset (the fencing token component)
 * @param accountNumber    account being changed
 * @param amount           signed delta applied
 * @param reason           user-facing statement description
 * @param resultingBalance the in-memory balance after applying {@code amount}
 */
public record LedgerChange(
        String topic,
        int partition,
        long offset,
        String accountNumber,
        long amount,
        String reason,
        long resultingBalance) {
}
