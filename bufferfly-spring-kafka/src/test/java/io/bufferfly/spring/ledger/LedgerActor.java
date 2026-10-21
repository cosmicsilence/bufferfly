package io.bufferfly.spring.ledger;

import io.bufferfly.core.persistence.AbstractPersistentActor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;

import java.util.HashMap;
import java.util.Map;

/**
 * A persistent, per-partition actor that maintains account balances <b>in
 * memory</b> and streams every change to the database through the BufferFly
 * persistence runner.
 *
 * <h2>Projected (actor) vs. confirmed (service)</h2>
 * <ul>
 *   <li><b>projected</b> — a tally advanced eagerly in {@link #apply} as each
 *       message is received, used to stamp {@link LedgerChange#resultingBalance()}
 *       onto the row that will be written. It is owned exclusively by the single
 *       actor thread, so it is a plain {@link HashMap} with no synchronization.</li>
 *   <li><b>confirmed</b> — <em>not</em> held by the actor. The actor only holds a
 *       reference to a {@link BalanceConfirmationService} and, via a post-commit
 *       {@link PersistenceRunner.SideEffect side effect}, forwards each durably
 *       persisted change to it. The service owns the confirmed state and can be
 *       inspected independently (monitoring, an API, tests). This mirrors a
 *       real-world read-model / projection updated only after commit.</li>
 * </ul>
 *
 * <h2>Restart / offset rewind</h2>
 * On {@link #preStart()} the actor asks the {@link KafkaContext} to seek to the
 * next offset after the one durably stored in the DB (looked up by the
 * {@link LedgerOperations}). This resumes exactly where the previous instance
 * left off — no replay of already-persisted changes, no gaps.
 */
public final class LedgerActor extends AbstractPersistentActor<ConsumedRecord<AddToAccount>, LedgerChange> {

    private final String name;
    private final KafkaContext kafkaContext;
    private final LedgerOperations operations;
    private final BalanceConfirmationService confirmations;

    /** Running total advanced on receipt (used to stamp the row). Actor-thread only. */
    private final Map<String, Long> projected = new HashMap<>();

    public LedgerActor(String name,
                       KafkaContext kafkaContext,
                       LedgerOperations operations,
                       BalanceConfirmationService confirmations,
                       PersistenceConfig<LedgerChange> config) {
        super(config);
        this.name = name;
        this.kafkaContext = kafkaContext;
        this.operations = operations;
        this.confirmations = confirmations;
    }

    @Override
    protected void preStart() {
        // Rehydrate the in-memory projection from the authoritative DB balances,
        // so a restarted actor continues from the committed state rather than
        // from zero. Keeps projected == persisted across restarts.
        projected.putAll(operations.loadAllBalances());

        // Resume from the last durably committed offset (fencing token source).
        long stored = operations.loadStoredOffset(kafkaContext.topic(), kafkaContext.partition());
        if (stored < 0) {
            kafkaContext.seekToBeginning(); // nothing persisted yet -> replay from start
        } else {
            kafkaContext.seek(stored + 1);  // next unprocessed record
        }
    }

    @Override
    public PersistenceRunner.PersistenceEvent<LedgerChange> apply(ConsumedRecord<AddToAccount> record) {
        AddToAccount cmd = record.value();
        long projectedBalance = projected.merge(cmd.accountNumber(), cmd.amount(), Long::sum);
        LedgerChange change = new LedgerChange(
                record.topic(), record.partition(), record.offset(),
                cmd.accountNumber(), cmd.amount(), cmd.reason(), projectedBalance);
        // Carry a side effect that confirms the change against the service once
        // (and only once) the batch is durably committed.
        return PersistenceRunner.newEvent(change, new BalanceApplied(cmd.accountNumber(), cmd.amount()));
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        return sideEffect -> {
            if (sideEffect instanceof BalanceApplied applied) {
                // Runs on the persistence thread, after commit -> forward to the
                // confirmation service (which owns the confirmed state).
                confirmations.confirm(applied.accountNumber(), applied.amount());
            }
        };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onError(ConsumedRecord<AddToAccount> record, Exception e) {
        // In a real system: metric/log. Kept minimal for the showcase.
    }

    /** The projected (on-receipt) balance for an account. */
    public long projectedBalance(String accountNumber) {
        return projected.getOrDefault(accountNumber, 0L);
    }

    /** A post-commit signal that a persisted change should be confirmed. */
    public record BalanceApplied(String accountNumber, long amount)
            implements PersistenceRunner.SideEffect {
    }
}
