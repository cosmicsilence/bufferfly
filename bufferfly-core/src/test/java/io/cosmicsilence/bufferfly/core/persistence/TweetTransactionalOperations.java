package io.cosmicsilence.bufferfly.core.persistence;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concrete {@link TransactionalOperations} implementation for {@link Tweet} entities using
 * Spring's {@link JdbcTemplate} and {@link TransactionTemplate}.
 *
 * <p>Leverages Spring's JDBC abstractions to:
 * <ul>
 *   <li>Execute atomic batch inserts with automatic transaction rollback on failure.</li>
 *   <li>Translate low-level SQL exceptions into Spring's exception hierarchy.</li>
 *   <li>Classify transient errors (connection refused, timeouts, deadlocks) into {@link TransientException}.</li>
 *   <li>Classify data constraint violations (poison pills, e.g. text > 100 chars) into {@link SingleItemException}.</li>
 *   <li>Provide concise query helpers without JDBC boilerplate.</li>
 * </ul>
 */
public class TweetTransactionalOperations implements TransactionalOperations<Tweet> {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final String tableName;

    private final List<Tweet> poisonPills = new CopyOnWriteArrayList<>();
    private final List<Exception> poisonPillCauses = new CopyOnWriteArrayList<>();

    private final AtomicInteger runCallCount = new AtomicInteger(0);
    private final AtomicInteger persistedCount = new AtomicInteger(0);
    private final AtomicInteger transientExceptionCount = new AtomicInteger(0);
    private final AtomicInteger singleItemExceptionCount = new AtomicInteger(0);

    public TweetTransactionalOperations(DataSource dataSource, String tableName) {
        if (dataSource == null) throw new NullPointerException("dataSource must not be null");
        this.tableName = (tableName != null) ? tableName : "tweet";
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
        this.transactionTemplate = new TransactionTemplate(txManager);
    }

    public TweetTransactionalOperations(DataSource dataSource) {
        this(dataSource, "tweet");
    }

    public TweetTransactionalOperations(String jdbcUrl, String user, String password, String tableName) {
        this(new DriverManagerDataSource(jdbcUrl, user, password), tableName);
    }

    public TweetTransactionalOperations(String jdbcUrl, String user, String password) {
        this(jdbcUrl, user, password, "tweet");
    }

    public TweetTransactionalOperations(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate, String tableName) {
        if (jdbcTemplate == null) throw new NullPointerException("jdbcTemplate must not be null");
        if (transactionTemplate == null) throw new NullPointerException("transactionTemplate must not be null");
        this.tableName = (tableName != null) ? tableName : "tweet";
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Creates the tweet table if it does not already exist.
     */
    public void createTableIfNotExists() {
        jdbcTemplate.execute("CREATE TABLE " + tableName + " (id VARCHAR(50) PRIMARY KEY, text VARCHAR2(100))");
    }

    /**
     * Truncates the table.
     */
    public void truncateTable() {
        jdbcTemplate.execute("TRUNCATE TABLE " + tableName);
    }

    @Override
    public void run(List<Tweet> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        runCallCount.incrementAndGet();

        try {
            transactionTemplate.execute(status -> {
                jdbcTemplate.batchUpdate(
                        "INSERT INTO " + tableName + " (id, text) VALUES (?, ?)",
                        batch,
                        batch.size(),
                        (ps, tweet) -> {
                            ps.setString(1, tweet.id());
                            ps.setString(2, tweet.text());
                        }
                );
                return null;
            });
            persistedCount.addAndGet(batch.size());
        } catch (DataAccessException | TransactionException e) {
            handleException(e, batch);
        } catch (Exception e) {
            handleException(e, batch);
        }
    }

    @Override
    public void onPoisonPill(Tweet event, Exception cause) {
        poisonPills.add(event);
        poisonPillCauses.add(cause);
    }

    private void handleException(Exception e, List<Tweet> batch) {
        if (isTransient(e)) {
            transientExceptionCount.incrementAndGet();
            throw new TransientException("Transient database failure: " + e.getMessage(), e);
        }
        // Non-transient data constraint / truncation violation (poison pill)
        singleItemExceptionCount.incrementAndGet();
        throw new SingleItemException("Data violation in batch (size " + batch.size() + "): " + e.getMessage(), e);
    }

    /**
     * Determines whether an exception represents a transient failure that can be retried.
     */
    private boolean isTransient(Exception e) {
        if (e instanceof CannotCreateTransactionException) return true;
        if (e instanceof CannotGetJdbcConnectionException) return true;
        if (e instanceof TransientDataAccessException) return true;
        if (e instanceof QueryTimeoutException) return true;
        if (e instanceof CannotAcquireLockException) return true;

        Throwable current = e;
        while (current != null) {
            if (current instanceof SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if (sqlState != null) {
                    if (sqlState.startsWith("08")) return true; // Connection exception (08001, 08003, etc.)
                    if (sqlState.startsWith("40")) return true; // Transaction rollback / deadlock
                }
                String msg = sqlEx.getMessage();
                if (msg != null) {
                    String lower = msg.toLowerCase();
                    if (lower.contains("connection") || lower.contains("network")
                            || lower.contains("socket") || lower.contains("refused")
                            || lower.contains("timeout") || lower.contains("closed")
                            || lower.contains("broken pipe") || lower.contains("reset by peer")) {
                        return true;
                    }
                }
            }
            if (current instanceof IOException || current instanceof SocketException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // Query / Assertion helpers using JdbcTemplate
    // -----------------------------------------------------------------------

    public int countTweets() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Integer.class);
        return count != null ? count : 0;
    }

    public Tweet findTweetById(String id) {
        List<Tweet> list = jdbcTemplate.query(
                "SELECT id, text FROM " + tableName + " WHERE id = ?",
                (rs, rowNum) -> new Tweet(rs.getString("id"), rs.getString("text")),
                id
        );
        return list.isEmpty() ? null : list.get(0);
    }

    public List<Tweet> getAllTweets() {
        return jdbcTemplate.query(
                "SELECT id, text FROM " + tableName + " ORDER BY id",
                (rs, rowNum) -> new Tweet(rs.getString("id"), rs.getString("text"))
        );
    }

    // -----------------------------------------------------------------------
    // Metrics and Inspector getters
    // -----------------------------------------------------------------------

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public List<Tweet> getPoisonPills() {
        return Collections.unmodifiableList(poisonPills);
    }

    public List<Exception> getPoisonPillCauses() {
        return Collections.unmodifiableList(poisonPillCauses);
    }

    public int getRunCallCount() {
        return runCallCount.get();
    }

    public int getPersistedCount() {
        return persistedCount.get();
    }

    public int getTransientExceptionCount() {
        return transientExceptionCount.get();
    }

    public int getSingleItemExceptionCount() {
        return singleItemExceptionCount.get();
    }
}
