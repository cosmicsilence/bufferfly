package io.bufferfly.spring.persistence;

import io.bufferfly.core.persistence.SingleItemException;
import io.bufferfly.core.persistence.TransactionalOperations;
import io.bufferfly.core.persistence.UnrecoverableException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reusable base for a JDBC-backed {@link TransactionalOperations} that persists
 * a natural batch <b>atomically</b>, advancing the stored consumer offset as an
 * optimistic-locking <b>fencing token</b> in the same transaction as the data.
 *
 * <p>This class owns the boilerplate every sharded-actor persistence layer
 * repeats: it sets up a {@link JdbcTemplate} and a <em>programmatic</em>
 * {@link TransactionTemplate} (over a {@link DataSourceTransactionManager}),
 * opens exactly one transaction per natural batch, fences+advances the offset,
 * invokes a single abstract {@link #persistBatch(List)} hook for the actual
 * writes, and classifies failures into the {@code PersistenceException} family
 * the runner understands. Implementors supply only the data writes.
 *
 * <h2>One transaction per natural batch</h2>
 * Every record in a batch belongs to the same partition (one actor per
 * partition), so {@link #persistOnTransaction(List)}:
 * <ol>
 *   <li>opens one transaction;</li>
 *   <li>fences the offset via {@link FencingOffsetRepository#fenceAndAdvance}
 *       using the <b>highest offset in the batch</b> — a failed CAS means a
 *       zombie actor is racing the new owner and aborts the transaction with an
 *       {@link UnrecoverableException};</li>
 *   <li>calls {@link #persistBatch(List)} to write the data (your bulk
 *       insert/upsert);</li>
 *   <li>commits, or rolls everything back on any failure so the offset never
 *       advances past data that was not durably written.</li>
 * </ol>
 *
 * <h2>Error signalling</h2>
 * <ul>
 *   <li>A failed fence surfaces as {@link UnrecoverableException} (propagated
 *       as-is).</li>
 *   <li>A real DB data violation ({@link DataIntegrityViolationException},
 *       e.g. a value too long for a column) is translated to
 *       {@link SingleItemException} so the runner isolates the batch
 *       item-by-item and dead-letters the offender. On a poison pill the base
 *       class's {@link #onPoisonPill} template calls the implementor's
 *       {@link #deadLetter} and advances the fencing token past the offending
 *       offset — atomically, in one transaction.</li>
 * </ul>
 * Override {@link #classify(RuntimeException)} to recognise additional
 * transient/poison-pill conditions for your database.
 *
 * <p>Implementors supply exactly two hooks: {@link #persistBatch(List)} (the
 * bulk data write) and {@link #deadLetter(Object, Exception)} (the dead-letter
 * sink). All transaction and fencing-token coordination is handled here.
 *
 * @param <E> the persistence event type; must expose its Kafka coordinates via
 *            {@link OffsetBearing}
 */
public abstract class AbstractJdbcTransactionalOperations<E extends OffsetBearing>
        implements TransactionalOperations<E> {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final FencingOffsetRepository offsets;

    /**
     * Creates operations over {@code dataSource} using the default
     * {@code consumer_offset} fencing schema.
     *
     * @param dataSource the JDBC data source
     */
    protected AbstractJdbcTransactionalOperations(DataSource dataSource) {
        this(dataSource, new FencingOffsetRepository(new JdbcTemplate(dataSource)));
    }

    /**
     * Creates operations over {@code dataSource} with a caller-supplied
     * fencing-offset repository (e.g. one configured with custom table/column
     * names).
     *
     * @param dataSource the JDBC data source
     * @param offsets    the fencing-offset repository (should share the same
     *                   {@link JdbcTemplate}/{@code DataSource})
     */
    protected AbstractJdbcTransactionalOperations(DataSource dataSource, FencingOffsetRepository offsets) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.offsets = offsets;
    }

    /** @return the shared {@link JdbcTemplate} (for subclass queries/writes). */
    protected final JdbcTemplate jdbc() {
        return jdbc;
    }

    /** @return the fencing-offset repository. */
    protected final FencingOffsetRepository offsets() {
        return offsets;
    }

    @Override
    public final void persistOnTransaction(List<E> batch) {
        if (batch == null || batch.isEmpty()) return;

        beforePersist(batch);

        String topic = batch.get(0).topic();
        int partition = batch.get(0).partition();
        long maxOffset = batch.stream().mapToLong(OffsetBearing::offset).max().orElseThrow();

        try {
            tx.executeWithoutResult(status -> {
                // 1) fence + advance the offset (optimistic lock / zombie guard),
                //    using the highest offset in the batch. We store the offset
                //    ONCE per batch (the max), not per event.
                offsets.fenceAndAdvance(topic, partition, maxOffset);
                // 2) implementor writes the data within this same transaction.
                persistBatch(batch);
            });
        } catch (UnrecoverableException e) {
            throw e; // fence failures stay unrecoverable
        } catch (RuntimeException e) {
            throw classify(e);
        }
    }

    /**
     * Writes the data for one natural batch. Called <b>inside</b> the active
     * transaction, <b>after</b> the offset has been fenced and advanced. Must
     * not open or commit a transaction. Throw a {@code PersistenceException}
     * (or any runtime exception — see {@link #classify}) to signal failure; the
     * enclosing transaction rolls back.
     *
     * @param batch the non-empty, same-partition batch to persist
     */
    protected abstract void persistBatch(List<E> batch);

    /**
     * Hook invoked at the very start of {@link #persistOnTransaction}, before
     * the transaction opens — e.g. to apply a test-only throttle. The default is
     * a no-op.
     *
     * @param batch the batch about to be persisted
     */
    protected void beforePersist(List<E> batch) {
        // no-op by default
    }

    /**
     * Translates an exception thrown from within the transaction into the
     * {@code PersistenceException} family the runner understands. The default
     * maps {@link DataIntegrityViolationException} to {@link SingleItemException}
     * (poison pill) and wraps anything else as {@link UnrecoverableException}.
     * ({@link UnrecoverableException}s from the fence are propagated before this
     * is ever called.) Override to recognise transient/poison conditions
     * specific to your database.
     *
     * @param e the exception thrown inside the transaction
     * @return the classified persistence exception to throw to the runner
     */
    protected RuntimeException classify(RuntimeException e) {
        if (e instanceof DataIntegrityViolationException) {
            // A real DB data violation (e.g. a value too long for the column).
            // SingleItemException makes the runner isolate the batch item-by-item:
            // good records persist and the offender is routed to onPoisonPill.
            return new SingleItemException(
                    "DB rejected a record in the batch (data violation): " + e.getMessage(), e);
        }
        return new UnrecoverableException(e);
    }

    /**
     * Dead-letters a poison pill — an item the database can never accept (e.g. a
     * value too long for a column) — to wherever this operations layer keeps its
     * dead-letter sink (a table, an in-memory list, a downstream topic, ...).
     *
     * <p>This is the only poison-pill work an implementor writes; the base class
     * drives the lifecycle in {@link #onPoisonPill}: it runs this method and the
     * fencing-token advance together in a single transaction, so the record is
     * dead-lettered and stepped over atomically. Called <b>inside</b> that
     * transaction; use {@link #jdbc()} for any writes and do not open or commit
     * a transaction here.
     *
     * @param event the poison-pill event
     * @param cause the exception thrown when attempting to persist it alone
     */
    protected abstract void deadLetter(E event, Exception cause);

    /**
     * Template method: dead-letters the poison pill and advances the fencing
     * token past it, atomically, in a single transaction.
     *
     * <p>Advancing the stored offset past the poison pill is essential: after the
     * runner dead-letters the pill and restarts the actor, the actor's
     * {@code preStart} rewinds to {@code storedOffset + 1} and resumes
     * <em>after</em> the offender — rather than landing on it again and
     * re-delivering (and re-inserting) the surrounding good records, which would
     * duplicate data and loop the restart.
     *
     * <p>Implementors supply only {@link #deadLetter(Object, Exception)}; the
     * dead-letter write and the offset advance share one transaction, so either
     * both happen or neither does.
     *
     * @param event the poison-pill event
     * @param cause the exception thrown when attempting to persist it alone
     */
    @Override
    public final void onPoisonPill(E event, Exception cause) {
        tx.executeWithoutResult(status -> {
            deadLetter(event, cause);
            // step the fencing token over the dead-lettered offset (same tx)
            offsets.fenceAndAdvance(event.topic(), event.partition(), event.offset());
        });
    }

    /**
     * Runs {@code work} in a fresh transaction (convenience for subclasses that
     * need to coordinate several writes atomically outside the batch path).
     *
     * @param work the unit of work to run transactionally
     */
    protected final void inTransaction(Consumer<org.springframework.transaction.TransactionStatus> work) {
        tx.executeWithoutResult(work::accept);
    }

    /** @return the stored latest offset for the partition, or {@code -1} if none yet. */
    public final long loadStoredOffset(String topic, int partition) {
        return offsets.loadStoredOffset(topic, partition);
    }
}
