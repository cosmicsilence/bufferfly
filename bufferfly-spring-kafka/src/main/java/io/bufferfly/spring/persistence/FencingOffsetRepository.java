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
 *   FENCE_OFFSET(NAM_TOPIC VARCHAR, NUM_PARTITION INT, NUM_OFFSET BIGINT,
 *                DAT_CREATED TIMESTAMP, PRIMARY KEY(NAM_TOPIC, NUM_PARTITION))
 * </pre>
 * The table and column names are configurable via
 * {@link #FencingOffsetRepository(JdbcTemplate, String, String, String, String, String)}
 * so the repository can be reused against an existing schema. Ready-to-run DDL
 * for Postgres, MySQL (InnoDB) and Oracle lives under
 * {@code src/test/resources/db/fence_offset/}.
 *
 * <p>All methods MUST be called inside an active transaction (the caller owns
 * the transaction boundary); they use the supplied {@link JdbcTemplate} and do
 * not open or commit transactions themselves.
 */
public final class FencingOffsetRepository {

    /** Default offset table name. */
    public static final String DEFAULT_TABLE = "FENCE_OFFSET";
    /** Default topic column name. */
    public static final String DEFAULT_TOPIC_COLUMN = "NAM_TOPIC";
    /** Default partition column name. */
    public static final String DEFAULT_PARTITION_COLUMN = "NUM_PARTITION";
    /** Default latest-offset column name. */
    public static final String DEFAULT_OFFSET_COLUMN = "NUM_OFFSET";
    /** Default created-timestamp column name. */
    public static final String DEFAULT_CREATED_COLUMN = "DAT_CREATED";

    private final JdbcTemplate jdbc;
    private final String selectSql;
    private final String insertSql;
    private final String updateSql;

    /**
     * Creates a repository targeting the default {@code FENCE_OFFSET} schema.
     *
     * @param jdbc the JDBC template (shares the caller's DataSource/transaction)
     */
    public FencingOffsetRepository(JdbcTemplate jdbc) {
        this(jdbc, DEFAULT_TABLE, DEFAULT_TOPIC_COLUMN, DEFAULT_PARTITION_COLUMN,
                DEFAULT_OFFSET_COLUMN, DEFAULT_CREATED_COLUMN);
    }

    /**
     * Creates a repository targeting a custom table/column layout, using the
     * default {@code DAT_CREATED} timestamp column name.
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
        this(jdbc, table, topicColumn, partitionColumn, offsetColumn, DEFAULT_CREATED_COLUMN);
    }

    /**
     * Creates a repository targeting a fully custom table/column layout.
     *
     * @param jdbc            the JDBC template (shares the caller's DataSource/transaction)
     * @param table           the offset table name
     * @param topicColumn     the topic column name
     * @param partitionColumn the partition column name
     * @param offsetColumn    the latest-offset column name
     * @param createdColumn   the created-timestamp column name (set once on insert)
     */
    public FencingOffsetRepository(JdbcTemplate jdbc,
                                   String table,
                                   String topicColumn,
                                   String partitionColumn,
                                   String offsetColumn,
                                   String createdColumn) {
        this.jdbc = jdbc;
        this.selectSql = "SELECT %s FROM %s WHERE %s = ? AND %s = ?"
                .formatted(offsetColumn, table, topicColumn, partitionColumn);
        // DAT_CREATED is stamped once, at insert time, with the DB clock.
        this.insertSql = "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, CURRENT_TIMESTAMP)"
                .formatted(table, topicColumn, partitionColumn, offsetColumn, createdColumn);
        this.updateSql = "UPDATE %s SET %s = ? WHERE %s = ? AND %s = ? AND %s = ?"
                .formatted(table, offsetColumn, topicColumn, partitionColumn, offsetColumn);
    }

    /**
     * Advances the stored offset for {@code (topic, partition)} from the
     * authoritative {@code currentOffset} the caller holds to {@code newMax}
     * using a single guarded (compare-and-set) write — <b>without reading the
     * stored value first</b>. {@code currentOffset} <em>is</em> the optimistic
     * lock: the caller (the persistence runner) seeded it from the durable store
     * when the actor (re)started and has advanced it after every committed batch,
     * so it is the token this writer provably holds. Returns normally only if the
     * CAS matched; otherwise throws {@link UnrecoverableException} so the
     * enclosing transaction rolls back and the actor is halted/restarted.
     *
     * <p>Reading the stored offset here (the previous design) reintroduced the
     * very race a fencing token exists to close: between the read and the write a
     * zombie could advance the token, yet the CAS would still be issued against
     * the stale read. By taking the token as a parameter instead, a zombie actor
     * — which holds an <em>older</em> {@code currentOffset} — can never match the
     * row a live owner already advanced, so its write is rejected.
     *
     * <ul>
     *   <li>{@code currentOffset < 0} — no token held yet (first batch ever for
     *       the partition): insert the row. If the insert does not affect exactly
     *       one row, a concurrent writer inserted first → zombie race →
     *       {@link UnrecoverableException}.</li>
     *   <li>{@code newMax <= currentOffset} — a benign replay at/behind the token
     *       we already hold (e.g. a redelivery whose data the idempotent sink has
     *       already absorbed). Not a zombie and not a backwards CAS: a safe no-op,
     *       the token is left unchanged.</li>
     *   <li>otherwise — compare-and-set from {@code currentOffset} to
     *       {@code newMax} ({@code WHERE latest_offset = currentOffset}). Zero
     *       rows updated means a zombie advanced the token concurrently →
     *       {@link UnrecoverableException}.</li>
     * </ul>
     *
     * @param topic         the partition's topic
     * @param partition     the partition
     * @param currentOffset the authoritative token the caller holds
     *                      ({@code < 0} when nothing has been committed yet)
     * @param newMax        the highest offset in the batch being persisted
     * @throws UnrecoverableException on a zombie race (failed fence)
     */
    public void fenceAndAdvance(String topic, int partition, long currentOffset, long newMax) {
        if (currentOffset < 0) {
            int inserted = jdbc.update(insertSql, topic, partition, newMax);
            if (inserted != 1) {
                throw new UnrecoverableException(
                        "Zombie race inserting offset token for " + topic + "-" + partition
                                + "; halting to restart clean");
            }
            return;
        }

        if (newMax <= currentOffset) {
            // At/behind the token we already hold: a benign replay. The sink is
            // idempotent by (topic, partition, offset), so this is a safe no-op —
            // no CAS (which would be a no-op or a backwards move) and NOT a zombie.
            return;
        }

        int updated = jdbc.update(updateSql, newMax, topic, partition, currentOffset);
        if (updated != 1) {
            throw new UnrecoverableException(
                    ("Offset fence failed for %s-%d: expected latest_offset=%d but it was advanced "
                            + "concurrently — a zombie actor is racing the new owner. Halting to restart clean.")
                            .formatted(topic, partition, currentOffset));
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
