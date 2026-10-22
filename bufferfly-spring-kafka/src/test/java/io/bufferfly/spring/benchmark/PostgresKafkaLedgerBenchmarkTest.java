package io.bufferfly.spring.benchmark;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 <b>Benchmark</b>: BufferFly actor (natural batching) vs. a classic
 * multi-threaded locking consumer — both ingesting the <em>same</em> Kafka
 * workload over <b>3 topics</b> into the <em>same</em> real <b>Postgres</b>,
 * run on throwaway Docker containers via Testcontainers.
 *
 * <h2>What is compared</h2>
 * <table>
 *   <tr><th>&nbsp;</th><th>Classic</th><th>BufferFly actor</th></tr>
 *   <tr><td>Consumers</td><td>1 per topic + a {@code 7}-thread pool each</td>
 *       <td>1 per topic, routing to one actor per (topic,partition)</td></tr>
 *   <tr><td>Sum calc</td><td>per-account {@link java.util.concurrent.locks.ReentrantLock}</td>
 *       <td>single-writer actor, no locks</td></tr>
 *   <tr><td>DB writes</td><td>one tiny transaction per message</td>
 *       <td>one bulk transaction per natural batch</td></tr>
 * </table>
 *
 * <h2>What is measured</h2>
 * End-to-end wall-clock (produce→all-persisted) plus Postgres server-side
 * {@code pg_stat_database.xact_commit} and {@code pg_stat_wal} deltas, so the
 * WAL/commit contention of the per-message path is visible next to the raw
 * throughput of both. Both runs must persist every message exactly once with
 * identical, correct balances — same result, very different DB cost.
 *
 * <h2>Graceful skip</h2>
 * {@code @Testcontainers(disabledWithoutDocker = true)} disables the whole class
 * when no Docker environment is available, so the benchmark never fails a build
 * on a machine (or CI stage) without Docker; it simply reports as skipped.
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(600)
@DisplayName("🦋 Benchmark: actor natural-batching vs classic locking consumer over Postgres + Kafka")
class PostgresKafkaLedgerBenchmarkTest {

    private static final List<String> TOPICS = List.of("ledger-a", "ledger-b", "ledger-c");
    private static final int MESSAGES_PER_TOPIC = 50_000;      // 150k total
    private static final int ACCOUNTS_PER_TOPIC = 500;
    private static final int ACTOR_BATCH_SIZE = 500;
    private static final long ACTOR_TIMEOUT_MS = 20L;
    private static final String DB_NAME = "ledger";
    // The classic path runs TOPICS.size() x ClassicLockingConsumer.THREADS_PER_TOPIC
    // (3 x 7 = 21) worker threads, each doing a tiny transaction at a time, plus
    // the main thread's stats/assertion queries. Size the pool to cover all of
    // them so a worker never blocks waiting to borrow a connection — the pool,
    // not connection setup, is what we want the classic path to exercise.
    private static final int DB_POOL_SIZE = 24;

    @Container
    @SuppressWarnings("resource")
    static final org.testcontainers.postgresql.PostgreSQLContainer POSTGRES =
            new org.testcontainers.postgresql.PostgreSQLContainer("postgres:16-alpine")
                    .withDatabaseName(DB_NAME)
                    .withUsername("bench")
                    .withPassword("bench")
                    // a little more headroom + reset WAL stats visibility; keep it light
                    .withCommand("postgres", "-c", "max_connections=200", "-c", "fsync=on");

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA =
            new KafkaContainer("apache/kafka-native:3.8.0");

    @Test
    @DisplayName("both approaches persist every message exactly once; WAL/commit cost and throughput are reported")
    void benchmark_actor_vs_classic() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        createTopics(bootstrap);

        HikariDataSource dataSource = newPool();
        try {
            runBenchmark(bootstrap, dataSource);
        } finally {
            dataSource.close();
        }
    }

    private void runBenchmark(String bootstrap, DataSource dataSource) throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        long expectedTotal = (long) MESSAGES_PER_TOPIC * TOPICS.size();

        // --- produce the shared workload once; both approaches read it with their
        //     own consumer group from the beginning ---
        try (BenchmarkProducer producer = new BenchmarkProducer(bootstrap)) {
            long produced = producer.produce(TOPICS, MESSAGES_PER_TOPIC, ACCOUNTS_PER_TOPIC);
            assertEquals(expectedTotal, produced, "produced message count");
        }

        // =====================================================================
        // 1) CLASSIC: 7-thread pool per topic, per-account lock, 1 tx/message
        // =====================================================================
        PgLedgerStore classicStore = new PgLedgerStore(dataSource, "classic");
        classicStore.createSchema();
        PgStats classicBefore = PgStats.capture(jdbc, DB_NAME);
        long classicMs;
        try (ClassicLockingConsumer classic =
                     new ClassicLockingConsumer(bootstrap, TOPICS, "classic-group", classicStore)) {
            classicMs = classic.runUntil(expectedTotal);
        }
        PgStats classicDelta = PgStats.capture(jdbc, DB_NAME).minus(classicBefore);

        // =====================================================================
        // 2) ACTOR: one actor per (topic,partition), natural batching, bulk tx
        // =====================================================================
        PgLedgerStore actorStore = new PgLedgerStore(dataSource, "actor");
        actorStore.createSchema();
        PgActorLedgerOperations actorOps = new PgActorLedgerOperations(dataSource, actorStore);
        PgStats actorBefore = PgStats.capture(jdbc, DB_NAME);
        long actorMs;
        try (ActorLedgerConsumer actor = new ActorLedgerConsumer(
                bootstrap, TOPICS, "actor-group", actorOps, ACTOR_BATCH_SIZE, ACTOR_TIMEOUT_MS)) {
            actorMs = actor.runUntil(actorStore, expectedTotal);
        }
        PgStats actorDelta = PgStats.capture(jdbc, DB_NAME).minus(actorBefore);

        // =====================================================================
        // 3) CORRECTNESS: both persisted every message exactly once, identically
        // =====================================================================
        assertEquals(expectedTotal, classicStore.totalChanges(), "classic must persist every message exactly once");
        assertEquals(expectedTotal, actorStore.totalChanges(), "actor must persist every message exactly once");
        assertEquals(classicStore.totalBalance(), actorStore.totalBalance(),
                "both approaches must compute the identical total balance");
        assertEquals(expectedTotal, classicStore.totalBalance(),
                "every credit is +1, so total balance must equal the message count");
        assertEquals(classicStore.distinctAccounts(), actorStore.distinctAccounts(),
                "both must touch the same set of accounts");

        // =====================================================================
        // 4) REPORT
        // =====================================================================
        double classicTps = expectedTotal / (classicMs / 1000.0);
        double actorTps = expectedTotal / (actorMs / 1000.0);
        double speedup = (double) classicMs / Math.max(1, actorMs);
        double commitReduction = classicDelta.xactCommit() / (double) Math.max(1, actorDelta.xactCommit());
        double walReduction = classicDelta.walBytes() / (double) Math.max(1, actorDelta.walBytes());

        System.out.println();
        System.out.println("==========================================================================================");
        System.out.printf ("POSTGRES + KAFKA LEDGER BENCHMARK  —  %d messages over %d topics%n", expectedTotal, TOPICS.size());
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf ("%-34s %16s %16s%n", "", "CLASSIC (locks)", "ACTOR (batching)");
        System.out.printf ("%-34s %16s %16s%n", "consumers", TOPICS.size() + "x7 threads", TOPICS.size() + " actors");
        System.out.printf ("%-34s %14.2f ms %14.2f ms%n", "end-to-end time", (double) classicMs, (double) actorMs);
        System.out.printf ("%-34s %13.0f/s %13.0f/s%n", "throughput", classicTps, actorTps);
        System.out.printf ("%-34s %16d %16d%n", "pg xact_commit (delta)", classicDelta.xactCommit(), actorDelta.xactCommit());
        System.out.printf ("%-34s %16d %16d%n", "pg wal_records (delta)", classicDelta.walRecords(), actorDelta.walRecords());
        System.out.printf ("%-34s %16d %16d%n", "pg wal_bytes (delta)", classicDelta.walBytes(), actorDelta.walBytes());
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf ("Actor speedup:                 %6.2fx   |   commit reduction: %6.2fx   |   WAL reduction: %6.2fx%n",
                speedup, commitReduction, walReduction);
        System.out.println("==========================================================================================");
        System.out.println();

        // The whole thesis: the actor collapses N tiny commits into a handful of
        // fat ones, so it issues dramatically fewer commits than the classic path.
        assertTrue(actorDelta.xactCommit() < classicDelta.xactCommit(),
                "actor natural batching must issue far fewer commits than one-transaction-per-message");
    }

    /**
     * Builds the <b>shared</b> connection pool both approaches write through, so
     * the benchmark measures how each <em>drives</em> the database rather than
     * the cost of opening connections. The pool is sized to cover every classic
     * worker ({@link #DB_POOL_SIZE}) so the per-message path is never throttled
     * by connection acquisition — that is the whole point of fronting Postgres
     * with a pool instead of a {@code DriverManagerDataSource}.
     */
    private HikariDataSource newPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(DB_POOL_SIZE);
        config.setMinimumIdle(DB_POOL_SIZE);
        config.setPoolName("bench-pg-pool");
        config.setAutoCommit(true);
        return new HikariDataSource(config);
    }

    private void createTopics(String bootstrap) throws Exception {        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            // one partition per topic so each topic maps to exactly one actor
            // (single writer) on the actor side and a clean comparison overall.
            List<NewTopic> newTopics = TOPICS.stream()
                    .map(t -> new NewTopic(t, 1, (short) 1))
                    .toList();
            admin.createTopics(newTopics).all().get();
        }
    }
}
