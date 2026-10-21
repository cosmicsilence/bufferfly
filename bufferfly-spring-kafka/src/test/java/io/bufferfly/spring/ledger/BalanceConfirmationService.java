package io.bufferfly.spring.ledger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A standalone collaborator that records <b>confirmed</b> account balances —
 * i.e. the running totals accounted only <em>after</em> a change has been
 * durably persisted.
 *
 * <p>In a real system this is the kind of component a post-commit side effect
 * notifies: a read-model / projection store, a metrics sink, a cache warmer, or
 * a downstream notifier. It is deliberately decoupled from the
 * {@link LedgerActor} — the actor merely holds a reference and forwards each
 * committed change to it, and anything else (monitoring, an API, the showcase
 * test) can inspect it independently.
 *
 * <h2>Threading</h2>
 * {@link #confirm} is invoked on the persistence thread (from the side effect)
 * while {@link #balanceOf} may be read from any thread, so the backing map is a
 * {@link ConcurrentHashMap}. A single service instance may be shared across all
 * partition actors; account numbers are globally unique so there is no cross-
 * partition contention on the same key.
 */
public final class BalanceConfirmationService {

    private final Map<String, Long> confirmed = new ConcurrentHashMap<>();

    /**
     * Applies a durably-committed delta to an account's confirmed balance.
     *
     * @param accountNumber the account
     * @param amount        the signed delta that was just persisted
     * @return the account's new confirmed balance
     */
    public long confirm(String accountNumber, long amount) {
        return confirmed.merge(accountNumber, amount, Long::sum);
    }

    /** @return the confirmed balance for an account (0 if none confirmed yet). */
    public long balanceOf(String accountNumber) {
        return confirmed.getOrDefault(accountNumber, 0L);
    }

    /** @return the number of distinct accounts with a confirmed balance. */
    public int accountCount() {
        return confirmed.size();
    }
}
