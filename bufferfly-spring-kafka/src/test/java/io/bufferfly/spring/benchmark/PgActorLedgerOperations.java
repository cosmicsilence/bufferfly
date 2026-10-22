package io.bufferfly.spring.benchmark;

import io.bufferfly.spring.persistence.AbstractJdbcTransactionalOperations;
import io.bufferfly.spring.persistence.FencingOffsetRepository;

import javax.sql.DataSource;
import java.util.List;

/**
 * The actor approach's {@link io.bufferfly.core.persistence.TransactionalOperations}
 * for the benchmark — a thin adapter that dogfoods the main-source
 * {@link AbstractJdbcTransactionalOperations} (so the benchmark exercises the
 * real production base class, fencing token and all) and delegates the actual
 * bulk write to the <b>shared</b> {@link PgLedgerStore} used by both approaches.
 *
 * <p>The base class already opens one transaction per natural batch and advances
 * the fencing token; here we point it at this approach's prefixed offset table
 * and let {@link PgLedgerStore#applyBatch} do the bulk insert + balance upserts
 * inside that same transaction.
 */
public final class PgActorLedgerOperations extends AbstractJdbcTransactionalOperations<PgLedgerChange> {

    private final PgLedgerStore store;

    public PgActorLedgerOperations(DataSource dataSource, PgLedgerStore store) {
        super(dataSource, new FencingOffsetRepository(
                new org.springframework.jdbc.core.JdbcTemplate(dataSource),
                store.offsetTable(), "NAM_TOPIC", "NUM_PARTITION", "NUM_OFFSET", "DAT_CREATED"));
        this.store = store;
    }

    @Override
    protected void persistBatch(List<PgLedgerChange> batch) {
        store.applyBatch(batch);
    }

    @Override
    protected void deadLetter(PgLedgerChange event, Exception cause) {
        // The benchmark workload contains no poison pills; nothing to dead-letter.
        // (The base class still advances the fencing token past it.)
    }
}
