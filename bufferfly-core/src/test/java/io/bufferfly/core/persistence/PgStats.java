package io.bufferfly.core.persistence;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A snapshot of Postgres server-side counters used to quantify WAL and commit
 * pressure, so the two approaches in {@link PersistenceActorBenchmarkTest} can
 * be compared not just on wall-clock time but on how much work each made the
 * database actually do.
 *
 * <ul>
 *   <li>{@code xactCommit} — {@code pg_stat_database.xact_commit}: total
 *       transactions committed in this database. The direct path commits ~once
 *       per row; the actor path commits ~once per natural batch.</li>
 *   <li>{@code walBytes} — {@code pg_stat_wal.wal_bytes}: total WAL bytes
 *       generated (cluster-wide). More, smaller transactions amplify WAL via
 *       per-commit records and full-page writes.</li>
 *   <li>{@code walRecords} — {@code pg_stat_wal.wal_records}: total WAL records
 *       generated.</li>
 * </ul>
 * Take a snapshot {@link #capture} before a run and another after; the
 * {@link #minus delta} isolates that run's contribution.
 */
public record PgStats(long xactCommit, long walBytes, long walRecords) {

    public static PgStats capture(JdbcTemplate jdbc, String databaseName) {
        long commits = firstLong(jdbc,
                "SELECT xact_commit FROM pg_stat_database WHERE datname = ?", databaseName);
        long walBytes = firstLong(jdbc, "SELECT wal_bytes FROM pg_stat_wal");
        long walRecords = firstLong(jdbc, "SELECT wal_records FROM pg_stat_wal");
        return new PgStats(commits, walBytes, walRecords);
    }

    /** @return this minus {@code before} — the delta attributable to a run. */
    public PgStats minus(PgStats before) {
        return new PgStats(
                xactCommit - before.xactCommit,
                walBytes - before.walBytes,
                walRecords - before.walRecords);
    }

    private static long firstLong(JdbcTemplate jdbc, String sql, Object... args) {
        Long v = jdbc.query(sql, rs -> rs.next() ? rs.getLong(1) : 0L, args);
        return v == null ? 0L : v;
    }
}
