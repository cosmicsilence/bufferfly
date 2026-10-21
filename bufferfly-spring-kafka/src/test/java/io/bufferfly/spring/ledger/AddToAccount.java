package io.bufferfly.spring.ledger;

/**
 * The command sent to a {@code LedgerActor} via Kafka.
 *
 * @param accountNumber the account to adjust
 * @param amount        signed delta applied to the balance (negative = debit,
 *                      positive = credit)
 * @param reason        free-text description shown to the end user on their
 *                      statement (e.g. "salary", "coffee", "refund")
 */
public record AddToAccount(String accountNumber, long amount, String reason) {
}
