package io.bufferfly.spring.rewind;

import io.bufferfly.core.persistence.TransactionalOperations;
import io.bufferfly.core.persistence.UnrecoverableException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Persists {@link RewindMessage} batches to HSQLDB for the two-partition
 * mid-batch restart/rewind test, with the stored consumer offset acting as an
 * optimistic-locking fencing token (same discipline as the ledger showcase and
 * the resilience suite).
 *
 * <h2>Injected fault</h2>
 * To drive a restart <em>in the middle of a batch</em> on partition 0, this
 * operations layer throws {@link UnrecoverableException} <b>once</b> the first
 * time it is asked to persist a partition-0 batch whose offsets reach
 * {@link #failP0AtOffset}. The runner then halts and the actor's
 * {@code onUnrecoverable} hook ({@code ctx.restartActor()}) rewinds partition 0
 * to its last durably stored offset. On the retry after restart the fault is no
 * longer armed, so processing completes.
 *
 * <p>Idempotent inserts by {@code (partition, offset)} mean the rewind's replay
 * never double-inserts, so the final per-partition content is exactly-once.
 */
public final class RewindOperations implements TransactionalOperations<RewindMessage> {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String topic;

    /** Partition-0 offset at/after which the first persist attempt fails once. */
    private final long failP0AtOffset;
    /** Whether the one-shot fault is still armed. */
    private final AtomicInteger p0FailsRemaining;
    /** How many times the fault actually fired (for test assertions). */
    private final AtomicInteger p0FailuresTriggered = new AtomicInteger();

    public RewindOperations(DataSource dataSource, String topic, long failP0AtOffset) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.topic = topic;
        this.failP0AtOffset = failP0AtOffset;
        this.p0FailsRemaining = new AtomicInteger(failP0AtOffset >= 0 ? 1 : 0);
    }

    public void createSchema() {
        jdbc.execute("""
                CREATE TABLE rewind_message (
                    partition INTEGER NOT NULL,
                    offset_value BIGINT NOT NULL,
                    body VARCHAR(256) NOT NULL,
                    PRIMARY KEY (partition, offset_value)
                )""");
        jdbc.execute("""
                CREATE TABLE consumer_offset (
                    topic VARCHAR(128) NOT NULL,
                    partition INTEGER NOT NULL,
                    latest_offset BIGINT NOT NULL,
                    PRIMARY KEY (topic, partition)
                )""");
    }

    @Override
    public long persistOnTransaction(List<RewindMessage> batch, long currentToken) {
        if (batch == null || batch.isEmpty()) return currentToken;

        int partition = batch.get(0).partition();
        long maxOffset = batch.stream().mapToLong(RewindMessage::offset).max().orElseThrow();

        // Inject the one-shot mid-batch failure on partition 0: if this batch
        // reaches the fail offset and the fault is still armed, halt BEFORE any
        // write so the restart genuinely rewinds and replays.
        if (partition == 0 && maxOffset >= failP0AtOffset && p0FailsRemaining.get() > 0
                && p0FailsRemaining.getAndDecrement() > 0) {
            p0FailuresTriggered.incrementAndGet();
            throw new UnrecoverableException(
                    "injected one-shot mid-batch failure on partition 0 at offset >= " + failP0AtOffset);
        }

        tx.executeWithoutResult(status -> {
            fenceAndAdvanceOffset(topic, partition, currentToken, maxOffset);
            for (RewindMessage m : batch) {
                jdbc.update("""
                                INSERT INTO rewind_message (partition, offset_value, body)
                                SELECT ?, ?, ?
                                FROM (VALUES(0)) t
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM rewind_message
                                     WHERE partition = ? AND offset_value = ?)""",
                        m.partition(), m.offset(), m.body(),
                        m.partition(), m.offset());
            }
        });

        return Math.max(currentToken, maxOffset);
    }

    @Override
    public long onPoisonPill(RewindMessage event, long currentToken, Exception cause) {
        // This test produces no poison pills; advance the token past the offset
        // defensively so a stray bad record could not loop.
        tx.executeWithoutResult(status ->
                fenceAndAdvanceOffset(topic, event.partition(), currentToken, event.offset()));
        return Math.max(currentToken, event.offset());
    }

    private void fenceAndAdvanceOffset(String topic, int partition, long currentOffset, long newMax) {
        if (currentOffset < 0) {
            int inserted = jdbc.update(
                    "INSERT INTO consumer_offset (topic, partition, latest_offset) VALUES (?, ?, ?)",
                    topic, partition, newMax);
            if (inserted != 1) {
                throw new UnrecoverableException(
                        "Zombie race inserting offset token for " + topic + "-" + partition);
            }
            return;
        }
        if (newMax <= currentOffset) {
            return; // benign replay at/behind the held token; idempotent insert makes this safe
        }
        int updated = jdbc.update("""
                        UPDATE consumer_offset
                           SET latest_offset = ?
                         WHERE topic = ? AND partition = ? AND latest_offset = ?""",
                newMax, topic, partition, currentOffset);
        if (updated != 1) {
            throw new UnrecoverableException(
                    ("Offset fence failed for %s-%d: expected %d, advanced concurrently (zombie).")
                            .formatted(topic, partition, currentOffset));
        }
    }

    // -- query helpers ------------------------------------------------------

    public long loadStoredOffset(String topic, int partition) {
        Long stored = jdbc.query(
                "SELECT latest_offset FROM consumer_offset WHERE topic = ? AND partition = ?",
                rs -> rs.next() ? rs.getLong(1) : null,
                topic, partition);
        return stored == null ? -1L : stored;
    }

    public int countForPartition(int partition) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM rewind_message WHERE partition = ?", Integer.class, partition);
        return n == null ? 0 : n;
    }

    public int totalCount() {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM rewind_message", Integer.class);
        return n == null ? 0 : n;
    }

    /** @return the offsets stored for a partition, in order (to assert exactly-once + contiguity). */
    public List<Long> offsetsForPartition(int partition) {
        return jdbc.query(
                "SELECT offset_value FROM rewind_message WHERE partition = ? ORDER BY offset_value",
                (rs, rowNum) -> rs.getLong(1),
                partition);
    }

    /** @return how many times the injected one-shot P0 failure actually fired. */
    public int p0FailuresTriggered() {
        return p0FailuresTriggered.get();
    }
}
