package io.bufferfly.spring.benchmark;

import io.bufferfly.spring.persistence.AbstractJdbcTransactionalOperations;
import io.bufferfly.spring.persistence.FencingOffsetRepository;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The actor approach's {@link io.bufferfly.core.persistence.TransactionalOperations}
 * for the benchmark — a thin adapter that dogfoods the main-source
 * {@link AbstractJdbcTransactionalOperations} (so the benchmark exercises the
 * real production base class, fencing token and all) and delegates the actual
 * bulk write to the <b>shared</b> {@link PgLedgerStore} used by every approach.
 *
 * <p>The base class already opens one transaction per natural batch and advances
 * the fencing token; here we point it at this approach's prefixed offset table
 * and let {@link PgLedgerStore#applyBatch} do the bulk insert + balance upserts
 * inside that same transaction.
 *
 * <p>{@link #persisted()} counts records up to the highest offset committed per
 * partition, so the benchmark can detect completion without querying the
 * database (polling queries would show up in the measured {@code xact_commit}).
 * It counts offsets rather than inserted rows so it stays exact when a commit
 * succeeds on the server but the client sees an error (e.g. an actor stopped by
 * a rebalance while waiting for the reply): the next owner replays those
 * records, inserts nothing, and still advances the offset.
 */
public final class PgActorLedgerOperations extends AbstractJdbcTransactionalOperations<PgLedgerChange> {

    private final PgLedgerStore store;
    /** Highest committed offset per "topic/partition". */
    private final Map<String, Long> committedOffsets = new ConcurrentHashMap<>();
    /** Time from the start of each batch transaction to its commit, summed over all partitions. */
    private final AtomicLong dbNanos = new AtomicLong();
    /** Start of the batch currently being persisted, per persistence thread. */
    private final ThreadLocal<Long> batchStart = new ThreadLocal<>();

    /** @return the shared store this operations layer writes to. */
    public PgLedgerStore store() {
        return store;
    }

    public PgActorLedgerOperations(DataSource dataSource, PgLedgerStore store) {
        super(dataSource, new FencingOffsetRepository(
                new org.springframework.jdbc.core.JdbcTemplate(dataSource),
                store.offsetTable(), "NAM_TOPIC", "NUM_PARTITION", "NUM_OFFSET", "DAT_CREATED"));
        this.store = store;
    }

    /** @return records durably processed so far: per partition, the highest committed offset + 1. */
    public long persisted() {
        return committedOffsets.values().stream().mapToLong(offset -> offset + 1).sum();
    }

    /** @return ms spent in batch transactions (start to commit), summed over all partitions. */
    public long dbMillis() {
        return dbNanos.get() / 1_000_000L;
    }

    @Override
    protected void beforePersist(List<PgLedgerChange> batch) {
        batchStart.set(System.nanoTime());
    }

    @Override
    protected void persistBatch(List<PgLedgerChange> batch) {
        store.applyBatch(batch);
        long start = batchStart.get();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                dbNanos.addAndGet(System.nanoTime() - start);
                batch.forEach(change -> committedOffsets.merge(
                        change.topic() + "/" + change.partition(), change.offset(), Math::max));
            }
        });
    }

    @Override
    protected void deadLetter(PgLedgerChange event, Exception cause) {
        // The benchmark workload contains no poison pills; nothing to dead-letter.
        // (The base class still advances the fencing token past it.)
    }
}
