package io.bufferfly.spring.resilience;

import io.bufferfly.core.persistence.SingleItemException;
import io.bufferfly.core.persistence.TransactionalOperations;
import io.bufferfly.core.persistence.TransientException;
import io.bufferfly.core.persistence.UnrecoverableException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Persists {@link Message} batches to HSQLDB for the resilience suite, one
 * transaction per natural batch, with the stored consumer offset acting as an
 * optimistic-locking fencing token (same discipline as the ledger showcase).
 *
 * <h2>Schema</h2>
 * <pre>
 *   message(id BIGINT, partition INT, offset_value BIGINT, body VARCHAR(256),
 *           PRIMARY KEY(partition, id))
 *   consumer_offset(topic, partition, latest_offset, PRIMARY KEY(topic, partition))
 * </pre>
 *
 * <h2>What it exercises</h2>
 * <ul>
 *   <li><b>Id auto-increment per partition.</b> The actor assigns a monotonically
 *       increasing {@code id} per partition; this layer stores it. On restart the
 *       actor rehydrates the next id from {@link #maxId(int)}.</li>
 *   <li><b>Poison pill.</b> A {@code body} longer than the column allows is
 *       rejected by HSQLDB (strict size) as a {@link DataIntegrityViolationException},
 *       surfaced as a {@link SingleItemException}; the runner isolates it and calls
 *       {@link #onPoisonPill}, which records it to an <em>in-memory</em> list
 *       instead of the DB (the suite's dead-letter sink).</li>
 *   <li><b>Backpressure.</b> {@link #withPersistDelay(int, long)} throttles a chosen
 *       partition's writes so its buffer backs up and engages backpressure, while
 *       other partitions keep draining at full speed.</li>
 *   <li><b>Exactly-once.</b> Inserts are idempotent by {@code (partition, offset)}
 *       so a restart/rewind replay never double-inserts.</li>
 * </ul>
 */
public final class MessageOperations implements TransactionalOperations<Message> {

    /** Max length of the {@code body} column; a longer body is a poison pill. */
    public static final int BODY_MAX_LENGTH = 256;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String topic;

    /** Dead-lettered poison pills, kept in memory (the suite's DLQ). */
    private final List<Message> poisonPills = new CopyOnWriteArrayList<>();

    /** Per-partition artificial persist delay (ms); index = partition. 0 = none. */
    private final java.util.Map<Integer, Long> persistDelayByPartition = new java.util.concurrent.ConcurrentHashMap<>();

    public MessageOperations(DataSource dataSource, String topic) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.topic = topic;
    }

    public void createSchema() {
        jdbc.execute("""
                CREATE TABLE message (
                    id BIGINT NOT NULL,
                    partition INTEGER NOT NULL,
                    offset_value BIGINT NOT NULL,
                    body VARCHAR(256) NOT NULL,
                    PRIMARY KEY (partition, id)
                )""");
        jdbc.execute("""
                CREATE TABLE consumer_offset (
                    topic VARCHAR(128) NOT NULL,
                    partition INTEGER NOT NULL,
                    latest_offset BIGINT NOT NULL,
                    PRIMARY KEY (topic, partition)
                )""");
    }

    /**
     * Throttle a single partition's bulk writes by {@code delayMs} so its buffer
     * backs up and engages backpressure, while the other partitions keep up.
     */
    public MessageOperations withPersistDelay(int partition, long delayMs) {
        if (delayMs < 0) throw new IllegalArgumentException("delayMs must be >= 0");
        persistDelayByPartition.put(partition, delayMs);
        return this;
    }

    @Override
    public long persistOnTransaction(List<Message> batch, long currentToken) {
        if (batch == null || batch.isEmpty()) return currentToken;

        int partition = batch.get(0).partition();

        long delay = persistDelayByPartition.getOrDefault(partition, 0L);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransientException("interrupted while throttling persist", e);
            }
        }

        long maxOffset = batch.stream().mapToLong(Message::offset).max().orElseThrow();
        String topic = topicOf(partition);

        try {
            tx.executeWithoutResult(status -> {
                // 1) fence + advance the offset: CAS from the authoritative token
                //    the runner holds (currentToken) to the batch max. No read.
                fenceAndAdvanceOffset(topic, partition, currentToken, maxOffset);

                // 2) idempotent insert by (partition, offset). An over-length body
                //    trips the strict-size VARCHAR and is caught below as a poison pill.
                for (Message m : batch) {
                    jdbc.update("""
                                    INSERT INTO message (id, partition, offset_value, body)
                                    SELECT ?, ?, ?, ?
                                    FROM (VALUES(0)) t
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM message
                                         WHERE partition = ? AND offset_value = ?)""",
                            m.id(), m.partition(), m.offset(), m.body(),
                            m.partition(), m.offset());
                }
            });
        } catch (UnrecoverableException e) {
            throw e;
        } catch (DataIntegrityViolationException e) {
            // A real DB data violation (e.g. body too long). Isolate item-by-item:
            // good records persist, the offender is dead-lettered via onPoisonPill.
            throw new SingleItemException(
                    "DB rejected a record in the batch (data violation): " + e.getMessage(), e);
        }

        return Math.max(currentToken, maxOffset);
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
            return; // benign replay at/behind the token we hold; idempotent insert makes this a safe no-op
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

    @Override
    public long onPoisonPill(Message event, long currentToken, Exception cause) {
        // Record to the in-memory DLQ (NOT the DB). Also advance the fencing token
        // past this offset so a restart's rewind resumes AFTER the poison pill
        // rather than re-delivering it (which would loop). CAS from the token the
        // runner handed us — no read, no internal tracking.
        poisonPills.add(event);
        tx.executeWithoutResult(status ->
                fenceAndAdvanceOffset(topicOf(event.partition()), event.partition(), currentToken, event.offset()));
        // Hand back the advanced token so the runner does not go stale.
        return Math.max(currentToken, event.offset());
    }

    // -- query helpers ------------------------------------------------------

    /** The topic this operations instance is bound to; offset tokens are keyed by (topic, partition). */
    private String topicOf(int partition) {
        return topic;
    }

    /** @return the number of poison pills dead-lettered to the in-memory list. */
    public int poisonPillCount() {
        return poisonPills.size();
    }

    /** @return the in-memory dead-letter list (poison pills). */
    public List<Message> poisonPills() {
        return poisonPills;
    }

    /** @return the highest stored id for a partition, or 0 if none (so next id = 1). */
    public long maxId(int partition) {
        Long max = jdbc.query(
                "SELECT MAX(id) FROM message WHERE partition = ?",
                rs -> rs.next() ? (Long) rs.getObject(1) : null,
                partition);
        return max == null ? 0L : max;
    }

    /** @return the stored latest offset for the partition, or -1 if none. */
    public long loadStoredOffset(String topic, int partition) {
        Long stored = jdbc.query(
                "SELECT latest_offset FROM consumer_offset WHERE topic = ? AND partition = ?",
                rs -> rs.next() ? rs.getLong(1) : null,
                topic, partition);
        return stored == null ? -1L : stored;
    }

    /** @return how many messages are stored for a partition. */
    public int countForPartition(int partition) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM message WHERE partition = ?", Integer.class, partition);
        return n == null ? 0 : n;
    }

    /** @return total messages stored across all partitions. */
    public int totalCount() {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM message", Integer.class);
        return n == null ? 0 : n;
    }

    /** @return the distinct bodies stored for a partition (to assert no loss/dup content). */
    public List<String> bodiesForPartition(int partition) {
        return jdbc.query(
                "SELECT body FROM message WHERE partition = ? ORDER BY id",
                (rs, rowNum) -> rs.getString(1),
                partition);
    }

    /** @return the ids stored for a partition in order (to assert contiguous auto-increment). */
    public List<Long> idsForPartition(int partition) {
        return jdbc.query(
                "SELECT id FROM message WHERE partition = ? ORDER BY id",
                (rs, rowNum) -> rs.getLong(1),
                partition);
    }
}
