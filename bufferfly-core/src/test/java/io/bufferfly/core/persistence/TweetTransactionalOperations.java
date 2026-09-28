package io.bufferfly.core.persistence;

import java.io.IOException;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concrete {@link TransactionalOperations} implementation for {@link Tweet} entities,
 * interacting with an HSQLDB database table {@code tweet (id VARCHAR(50) PRIMARY KEY, text VARCHAR2(100))}.
 *
 * <p>Handles:
 * <ul>
 *   <li>Atomic batch inserts with transaction commit/rollback.</li>
 *   <li>Poison pill detection (e.g., text exceeding 100 characters) triggering {@link SingleItemException}.</li>
 *   <li>Transient network/connection failures triggering {@link TransientException}.</li>
 *   <li>Dead-letter / poison-pill recording in {@link #onPoisonPill}.</li>
 * </ul>
 */
public class TweetTransactionalOperations implements TransactionalOperations<Tweet> {

    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection getConnection() throws SQLException;
    }

    private final ConnectionSupplier connectionSupplier;

    private final List<Tweet> poisonPills = Collections.synchronizedList(new ArrayList<>());
    private final List<Exception> poisonPillCauses = Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger runCallCount = new AtomicInteger(0);
    private final AtomicInteger persistedCount = new AtomicInteger(0);
    private final AtomicInteger transientExceptionCount = new AtomicInteger(0);
    private final AtomicInteger singleItemExceptionCount = new AtomicInteger(0);

    public TweetTransactionalOperations(ConnectionSupplier connectionSupplier) {
        if (connectionSupplier == null) throw new NullPointerException("connectionSupplier must not be null");
        this.connectionSupplier = connectionSupplier;
    }

    public TweetTransactionalOperations(String jdbcUrl, String user, String password) {
        this(() -> DriverManager.getConnection(jdbcUrl, user, password));
    }

    /**
     * Creates the tweet table if it does not already exist.
     */
    public void createTableIfNotExists() throws SQLException {
        try (Connection conn = connectionSupplier.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tweet (id VARCHAR(50) PRIMARY KEY, text VARCHAR2(100))");
        }
    }

    @Override
    public void run(List<Tweet> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        runCallCount.incrementAndGet();

        Connection conn;
        try {
            conn = connectionSupplier.getConnection();
        } catch (SQLException | RuntimeException e) {
            // Cannot connect to database -> database is unavailable -> throw TransientException
            transientExceptionCount.incrementAndGet();
            throw new TransientException("Database unavailable: " + e.getMessage(), e);
        }

        try (conn) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tweet (id, text) VALUES (?, ?)")) {
                for (Tweet tweet : batch) {
                    ps.setString(1, tweet.id());
                    ps.setString(2, tweet.text());
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                persistedCount.addAndGet(batch.size());
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    // Suppress secondary rollback failure
                }
                handleSqlException(e, batch);
            }
        } catch (SQLException e) {
            handleSqlException(e, batch);
        }
    }

    @Override
    public void onPoisonPill(Tweet event, Exception cause) {
        poisonPills.add(event);
        poisonPillCauses.add(cause);
    }

    private void handleSqlException(SQLException e, List<Tweet> batch) {
        if (isTransient(e)) {
            transientExceptionCount.incrementAndGet();
            throw new TransientException("Transient database failure: " + e.getMessage(), e);
        }
        // Non-transient data constraint / truncation violation (poison pill)
        singleItemExceptionCount.incrementAndGet();
        throw new SingleItemException("Data violation in batch (size " + batch.size() + "): " + e.getMessage(), e);
    }

    /**
     * Determines whether an SQL exception represents a transient failure (e.g. network disconnect,
     * timeout, deadlock) that can succeed on retry.
     */
    private boolean isTransient(SQLException e) {
        if (e instanceof java.sql.SQLTransientException) return true;
        if (e instanceof java.sql.SQLRecoverableException) return true;

        String sqlState = e.getSQLState();
        if (sqlState != null) {
            if (sqlState.startsWith("08")) return true; // Connection exception (08001, 08003, 08006, etc.)
            if (sqlState.startsWith("40")) return true; // Transaction rollback / deadlock
        }

        String msg = e.getMessage();
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (lower.contains("connection") || lower.contains("network")
                    || lower.contains("socket") || lower.contains("refused")
                    || lower.contains("timeout") || lower.contains("closed")
                    || lower.contains("broken pipe") || lower.contains("reset by peer")) {
                return true;
            }
        }

        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof IOException || cause instanceof SocketException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // Query / Assertion helpers
    // -----------------------------------------------------------------------

    public int countTweets() throws SQLException {
        try (Connection conn = connectionSupplier.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tweet")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    public Tweet findTweetById(String id) throws SQLException {
        try (Connection conn = connectionSupplier.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT id, text FROM tweet WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Tweet(rs.getString("id"), rs.getString("text"));
                }
                return null;
            }
        }
    }

    public List<Tweet> getAllTweets() throws SQLException {
        List<Tweet> list = new ArrayList<>();
        try (Connection conn = connectionSupplier.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, text FROM tweet ORDER BY id")) {
            while (rs.next()) {
                list.add(new Tweet(rs.getString("id"), rs.getString("text")));
            }
        }
        return list;
    }

    // -----------------------------------------------------------------------
    // Metrics and Inspector getters
    // -----------------------------------------------------------------------

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
