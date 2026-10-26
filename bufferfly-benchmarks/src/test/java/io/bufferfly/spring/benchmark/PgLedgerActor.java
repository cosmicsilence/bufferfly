package io.bufferfly.spring.benchmark;

import io.bufferfly.core.persistence.AbstractPersistentActor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.spring.ledger.AddToAccount;

import java.util.HashMap;
import java.util.Map;

/**
 * The BufferFly persistent actor for the benchmark: one per (topic, partition),
 * single-threaded, keeping the running balance <b>in memory</b> and streaming
 * each change to Postgres through the natural-batching persistence runner.
 *
 * <p>This is the same shape as the showcase {@code LedgerActor} but targets the
 * benchmark's {@link PgLedgerChange}/Postgres store. The actor loop is pure and
 * non-blocking — it merely merges the balance in a plain {@link HashMap} (owned
 * exclusively by the single actor thread, no locks) and hands the event to the
 * runner, which coalesces writes into bulk transactions.
 */
public final class PgLedgerActor extends AbstractPersistentActor<BenchmarkRecord, PgLedgerChange> {

    private final String name;
    private final Map<String, Long> projected = new HashMap<>();

    public PgLedgerActor(String name, PersistenceConfig<PgLedgerChange> config) {
        super(config);
        this.name = name;
    }

    @Override
    public PersistenceRunner.PersistenceEvent<PgLedgerChange> apply(BenchmarkRecord record) {
        AddToAccount cmd = record.value();
        long balance = projected.merge(cmd.accountNumber(), cmd.amount(), Long::sum);
        return PersistenceRunner.newEvent(new PgLedgerChange(
                record.topic(), record.partition(), record.offset(),
                cmd.accountNumber(), cmd.amount(), balance));
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        return sideEffect -> { /* no post-commit side effect needed for the benchmark */ };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onError(BenchmarkRecord record, Exception e) {
        // benchmark: a failure here would skew results; surface loudly
        throw new IllegalStateException("actor processing error for " + name, e);
    }
}
