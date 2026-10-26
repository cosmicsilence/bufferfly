package io.bufferfly.spring.benchmark;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.List;

/**
 * The <b>shared</b> Postgres ledger store used by <em>both</em> benchmark
 * approaches, so the two differ only in <em>how</em> they drive the database —
 * not in the schema, the SQL, or the DataSource.
 *
 * <h2>Schema (per approach, isolated by table prefix)</h2>
 * <pre>
 *   &lt;p&gt;_account_balance(account_number PK, balance BIGINT)
 *   &lt;p&gt;_account_change (id BIGSERIAL PK, account_number, amount, resulting_balance,
 *                            topic, partition, offset_value,
 *                            UNIQUE(topic, partition, offset_value))
 *   &lt;p&gt;_consumer_offset(topic, partition, latest_offset, PRIMARY KEY(topic, partition))
 * </pre>
 * Each approach gets its own prefixed set of tables ({@code actor_*} vs
 * {@code classic_*}) so a single run of one does not pollute the other and the
 * per-approach row counts/balances can be asserted independently against the
 * same Postgres instance.
 *
 * <h2>Two write paths, one store</h2>
 * <ul>
 *   <li>{@link #applyOneLocked} — the <b>classic</b> path: a single tiny
 *       transaction per message (BEGIN; INSERT change; UPSERT balance; UPSERT
 *       offset; COMMIT). This is the per-message-transaction pattern the actor
 *       is meant to replace; running it from a 7-thread pool per topic is what
 *       produces WAL/commit contention on Postgres.</li>
 *   <li>{@link #applyBatch} — the <b>actor</b> path: one transaction per
 *       <em>natural batch</em> (bulk insert of all changes, batched balance
 *       upserts, a single offset advance). Millions of tiny commits collapse
 *       into a handful of fat ones.</li>
 * </ul>
 */
public final class PgLedgerStore {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String prefix;
    private final String balanceTable;
    private final String changeTable;
    private final String offsetTable;

    public PgLedgerStore(DataSource dataSource, String tablePrefix) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.prefix = tablePrefix;
        this.balanceTable = tablePrefix + "_account_balance";
        this.changeTable = tablePrefix + "_account_change";
        this.offsetTable = tablePrefix + "_consumer_offset";
    }

    public JdbcTemplate jdbc() {
        return jdbc;
    }

    public String balanceTable() {
        return balanceTable;
    }

    public String changeTable() {
        return changeTable;
    }

    public String offsetTable() {
        return offsetTable;
    }

    /** Creates this approach's prefixed tables (idempotent — drops first). */
    public void createSchema() {
        jdbc.execute("DROP TABLE IF EXISTS " + changeTable);
        jdbc.execute("DROP TABLE IF EXISTS " + balanceTable);
        jdbc.execute("DROP TABLE IF EXISTS " + offsetTable);
        jdbc.execute("""
                CREATE TABLE %s (
                    account_number VARCHAR(64) PRIMARY KEY,
                    balance BIGINT NOT NULL
                )""".formatted(balanceTable));
        jdbc.execute("""
                CREATE TABLE %s (
                    id BIGSERIAL PRIMARY KEY,
                    account_number VARCHAR(64) NOT NULL,
                    amount BIGINT NOT NULL,
                    resulting_balance BIGINT NOT NULL,
                    topic VARCHAR(128) NOT NULL,
                    partition INTEGER NOT NULL,
                    offset_value BIGINT NOT NULL,
                    CONSTRAINT uq_%s UNIQUE (topic, partition, offset_value)
                )""".formatted(changeTable, prefix));
        jdbc.execute("""
                CREATE TABLE %s (
                    NAM_TOPIC VARCHAR(128) NOT NULL,
                    NUM_PARTITION INTEGER NOT NULL,
                    NUM_OFFSET BIGINT NOT NULL,
                    DAT_CREATED TIMESTAMP NOT NULL,
                    PRIMARY KEY (NAM_TOPIC, NUM_PARTITION)
                )""".formatted(offsetTable));
    }

    /**
     * The <b>classic</b> write path: persist a single change in its own tiny
     * transaction, computing the new balance from the current DB balance. The
     * caller (classic consumer) holds a per-account lock around this so the
     * read-modify-write of the balance is serialized per account — the
     * lock-based equivalent of the actor's single-writer discipline, but paying
     * one full transaction (and WAL flush/commit) per message.
     *
     * @return the resulting balance after applying {@code amount}
     */
    public long applyOneLocked(String topic, int partition, long offset,
                               String accountNumber, long amount) {
        return tx.execute(status -> {
            // append-only change, idempotent by (topic, partition, offset)
            int inserted = jdbc.update("""
                            INSERT INTO %s (account_number, amount, resulting_balance, topic, partition, offset_value)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT (topic, partition, offset_value) DO NOTHING
                            """.formatted(changeTable),
                    accountNumber, amount, 0L, topic, partition, offset);
            long current = balanceOf(accountNumber);
            long updated = current + (inserted > 0 ? amount : 0L);
            if (inserted > 0) {
                jdbc.update("""
                                INSERT INTO %s (account_number, balance) VALUES (?, ?)
                                ON CONFLICT (account_number) DO UPDATE SET balance = EXCLUDED.balance
                                """.formatted(balanceTable),
                        accountNumber, updated);
                // stamp the resulting balance on the change row we just inserted
                jdbc.update("""
                                UPDATE %s SET resulting_balance = ?
                                 WHERE topic = ? AND partition = ? AND offset_value = ?
                                """.formatted(changeTable),
                        updated, topic, partition, offset);
                // advance the stored offset (fencing token) in the same tx
                jdbc.update("""
                                INSERT INTO %s (NAM_TOPIC, NUM_PARTITION, NUM_OFFSET, DAT_CREATED)
                                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                                ON CONFLICT (NAM_TOPIC, NUM_PARTITION)
                                DO UPDATE SET NUM_OFFSET = GREATEST(%s.NUM_OFFSET, EXCLUDED.NUM_OFFSET)
                                """.formatted(offsetTable, offsetTable),
                        topic, partition, offset);
            }
            return updated;
        });
    }

    /**
     * The <b>actor</b> write path: persist a whole natural batch in one
     * transaction — bulk insert of changes (idempotent by
     * {@code (topic, partition, offset)}), per-account balance upserts, and a
     * single offset advance to the batch max. This is what turns N tiny commits
     * into one fat commit.
     */
    public void applyBatch(List<PgLedgerChange> batch) {
        if (batch == null || batch.isEmpty()) return;

        tx.executeWithoutResult(status -> {
            jdbc.batchUpdate("""
                            INSERT INTO %s (account_number, amount, resulting_balance, topic, partition, offset_value)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT (topic, partition, offset_value) DO NOTHING
                            """.formatted(changeTable),
                    batch.stream()
                            .map(c -> new Object[]{
                                    c.accountNumber(), c.amount(), c.resultingBalance(),
                                    c.topic(), c.partition(), c.offset()})
                            .toList());

            // one upsert per account with its final (last-in-batch) resulting balance
            // the batch is in offset order so the last write per account wins.
            batch.stream()
                    .collect(java.util.stream.Collectors.toMap(
                            PgLedgerChange::accountNumber,
                            c -> c,
                            (a, b) -> b,
                            java.util.LinkedHashMap::new))
                    .forEach((account, last) ->
                            jdbc.update("""
                                            INSERT INTO %s (account_number, balance) VALUES (?, ?)
                                            ON CONFLICT (account_number) DO UPDATE SET balance = EXCLUDED.balance
                                            """.formatted(balanceTable),
                                    account, last.resultingBalance()));
            // NOTE: the fencing offset (FENCE_OFFSET) is advanced by the actor's
            // FencingOffsetRepository in the SAME (outer) transaction, so we do
            // not touch the offset table here — that is the actor path's single
            // source of truth for the token.
        });
    }

    // -- query helpers -------------------------------------------------------

    public long balanceOf(String accountNumber) {
        Long balance = jdbc.query(
                "SELECT balance FROM " + balanceTable + " WHERE account_number = ?",
                rs -> rs.next() ? rs.getLong(1) : null,
                accountNumber);
        return balance == null ? 0L : balance;
    }

    public long totalBalance() {
        Long sum = jdbc.queryForObject("SELECT COALESCE(SUM(balance), 0) FROM " + balanceTable, Long.class);
        return sum == null ? 0L : sum;
    }

    public int totalChanges() {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM " + changeTable, Integer.class);
        return n == null ? 0 : n;
    }

    public int distinctAccounts() {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM " + balanceTable, Integer.class);
        return n == null ? 0 : n;
    }
}
