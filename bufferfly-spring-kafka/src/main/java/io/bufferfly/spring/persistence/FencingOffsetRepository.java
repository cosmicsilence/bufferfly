package io.bufferfly.spring.persistence;

import io.bufferfly.core.persistence.UnrecoverableException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A JDBC-backed repository for the per-partition consumer offset, used as an
 * optimistic-locking <b>fencing token</b>.
 *
 * <h2>Why a fencing token</h2>
 * A Kafka partition assignment is effectively a lease: after a rebalance a new
 * consumer owns the partition, but an old ("zombie") instance may still be
 * alive and about to write. A lease alone cannot make this safe — the fix is to
 * include a monotonically increasing token with every write and have the
 * storage layer reject any write whose token went backwards
 * (Kleppmann, <i>How to do distributed locking</i>). This repository is that
 * mechanism: {@code latest_offset} is advanced with a compare-and-set
 * ({@code WHERE latest_offset = :expected}) <em>in the same transaction as the
 * data</em>. A failed CAS means a zombie is racing the new owner, so the write
 * is rejected as {@link UnrecoverableException} and the actor restarts clean.
 *
 * <h2>Schema</h2>
 * By default this targets:
 * <pre>
 *   consumer_offset(topic VARCHAR, partition INT, latest_offset BIGINT,
 *                   PRIMARY KEY(topic, partition))
 * </pre>
 * The table and column names are configurable via
 * {@link #FencingOffsetRepository(JdbcTemplate, String, String, String, String)}
 * so the repository can be reused against an existing schema.
 *
 * <p>All methods MUST be called inside an active transaction (the caller owns
 * the transaction boundary); they use the supplied {@link JdbcTemplate} and do
 * not open or commit transactions themselves.
 */
public final class FencingOffsetRepository {

    /** Default offset table name. */
    public static final String DEFAULT_TABLE = "consumer_offset";
    /** Default topic column name. */
    public static final String DEFAULT_TOPIC_COLUMN = "topic";
    /** Default partition column name. */
    public static final String DEFAULT_PARTITION_COLUMN = "partition";
    /** Default latest-offset column name. */
    public static final String DEFAULT_OFFSET_COLUMN = "latest_offset";

    private final JdbcTemplate jdbc;
    private final String selectSql;
    private final String insertSql;
    private final String updateSql;

    /**
     * Creates a repository targeting the default {@code consumer_offset} schema.
     *
     * @param jdbc the JDBC template (shares the caller's DataSource/transaction)
     */
    public FencingOffsetRepository(JdbcTemplate jdbc) {
        this(jdbc, DEFAULT_TABLE, DEFAULT_TOPIC_COLUMN, DEFAULT_PARTITION_COLUMN, DEFAULT_OFFSET_COLUMN);
    }

    /**
     * Creates a repository targeting a custom table/column layout.
     *
     * @param jdbc            the JDBC template (shares the caller's DataSource/transaction)
     * @param table           the offset table name
     * @param topicColumn     the topic column name
     * @param partitionColumn the partition column name
     * @param offsetColumn    the latest-offset column name
     */
    public FencingOffsetRepository(JdbcTemplate jdbc,
                                   String table,
                                   String topicColumn,
                                   String partitionColumn,
                                   String offsetColumn) {
        this.jdbc = jdbc;
        this.selectSql = "SELECT %s FROM %s WHERE %s = ? AND %s = ?"
                .formatted(offsetColumn, table, topicColumn, partitionColumn);
        this.insertSql = "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)"
                .formatted(table, topicColumn, partitionColumn, offsetColumn);
        this.updateSql = "UPDATE %s SET %s = ? WHERE %s = ? AND %s = ? AND %s = ?"
                .formatted(table, offsetColumn, topicColumn, partitionColumn, offsetColumn);
    }

    /**
     * Advances the stored offset for {@code (topic, partition)} from its current
     * value to {@code newMax} using a guarded (compare-and-set) update. Returns
     * normally only if this writer held the authoritative token; otherwise
     * throws {@link UnrecoverableException} so the enclosing transaction rolls
     * back and the actor is halted/restarted.
     *
     * <ul>
     *   <li>No row yet — insert the token (first batch ever for the partition).
     *       If the insert does not affect exactly one row, a concurrent writer
     *       inserted first: zombie race → {@link UnrecoverableException}.</li>
     *   <li>{@code newMax <= stored} — a benign replay (e.g. redelivery after a
     *       restart/rewind). Idempotent sinks make this a safe no-op; it is
     *       <em>not</em> a zombie and the token is not advanced.</li>
     *   <li>{@code newMax > stored} — compare-and-set from {@code stored} to
     *       {@code newMax}. Zero rows updated means the token was advanced
     *       concurrently by a zombie → {@link UnrecoverableException}.</li>
     * </ul>
     *
     * @param topic     the partition's topic
     * @param partition the partition
     * @param newMax    the highest offset in the batch being persisted
     * @throws UnrecoverableException on a zombie race (failed fence)
     */
    public void fenceAndAdvance(String topic, int partition, long newMax) {
        Long stored = jdbc.query(selectSql,
                rs -> rs.next() ? rs.getLong(1) : null,
                topic, partition);

        if (stored == null) {
            int inserted = jdbc.update(insertSql, topic, partition, newMax);
            if (inserted != 1) {
                throw new UnrecoverableException(
                        "Zombie race inserting offset token for " + topic + "-" + partition
                                + "; halting to restart clean");
            }
            return;
        }

        if (newMax <= stored) {
            // At/behind the committed offset: a benign replay. The sink is
            // idempotent by (topic, partition, offset), so this is a safe no-op —
            // no advance, and crucially NOT treated as a zombie. A real zombie is
            // detected by the compare-and-set UPDATE below.
            return;
        }

        int updated = jdbc.update(updateSql, newMax, topic, partition, stored);
        if (updated != 1) {
            throw new UnrecoverableException(
                    ("Offset fence failed for %s-%d: expected latest_offset=%d but it was advanced "
                            + "concurrently — a zombie actor is racing the new owner. Halting to restart clean.")
                            .formatted(topic, partition, stored));
        }
    }

    /**
     * Reads the stored latest offset for a partition.
     *
     * @param topic     the topic
     * @param partition the partition
     * @return the stored latest offset, or {@code -1} if none is stored yet
     */
    public long loadStoredOffset(String topic, int partition) {
        Long stored = jdbc.query(selectSql,
                rs -> rs.next() ? rs.getLong(1) : null,
                topic, partition);
        return stored == null ? -1L : stored;
    }
}
