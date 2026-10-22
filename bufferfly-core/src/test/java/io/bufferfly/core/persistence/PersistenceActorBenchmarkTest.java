package io.bufferfly.core.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 <b>Benchmark</b> comparing, over a <b>real Postgres</b> (not an in-memory
 * HSQLDB), how an actor's natural batching drives the database versus the naive
 * per-row insert:
 * <ol>
 *   <li><b>Actor with natural batching</b>: {@link TweeterActor} enqueues
 *       messages via {@link BatchingPersistenceRunner}, which coalesces writes
 *       into natural bulk batches — one transaction per batch.</li>
 *   <li><b>{@value #VIRTUAL_THREADS} parallel virtual threads without
 *       batching</b>: each thread inserts tweets one-by-one via individual JDBC
 *       {@code update()} calls — one tiny transaction (and WAL flush/commit) per
 *       row.</li>
 * </ol>
 *
 * <h2>Why a real Postgres on a shared pool</h2>
 * Both approaches write through the <b>same HikariCP pool</b> into the <b>same
 * Postgres</b> (throwaway Docker container via Testcontainers), using the same
 * table shape and SQL. The only variable is <em>how</em> each drives the DB, so
 * the measured difference is the batching effect itself — not connection setup,
 * not a different store. Beyond wall-clock it captures Postgres's own
 * {@code pg_stat_database.xact_commit} and {@code pg_stat_wal} counters, so the
 * commit/WAL cost of the per-row path is visible next to the actor's bulk
 * transactions.
 *
 * <h2>Scenario</h2>
 * <ul>
 *   <li>{@value #TOTAL_TWEETS} tweets per scenario, into separate tables
 *       ({@code tweet_actor} vs {@code tweet_direct}) so the two runs never
 *       contend and each can be asserted independently.</li>
 *   <li>A warmup phase for both approaches precedes measurement so JIT, pool
 *       warmup and class loading do not skew results; warmed rows are truncated
 *       before the measured run.</li>
 *   <li>Full verification that all {@value #TOTAL_TWEETS} tweets are persisted
 *       in both scenarios.</li>
 * </ul>
 *
 * <h2>Graceful skip</h2>
 * {@code @Testcontainers(disabledWithoutDocker = true)} disables the whole class
 * when no Docker environment is available, so the benchmark never fails a build
 * on a machine (or CI stage) without Docker; it simply reports as skipped.
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
@DisplayName("🦋 Benchmark: actor natural-batching vs per-row inserts over a real Postgres")
class PersistenceActorBenchmarkTest {

    private static final int TOTAL_TWEETS = 100_000;
    private static final int WARMUP_TWEETS = 1_000;
    private static final int VIRTUAL_THREADS = 15;
    private static final String DB_NAME = "benchdb";
    // Sized to cover the VIRTUAL_THREADS concurrent per-row writers plus the
    // actor's single persistence connection and the main thread's stats/count
    // queries, so a writer never blocks waiting to borrow a connection — the
    // pool, not connection acquisition, is what the direct path exercises.
    private static final int DB_POOL_SIZE = 20;

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:16-alpine")
                    .withDatabaseName(DB_NAME)
                    .withUsername("bench")
                    .withPassword("bench")
                    .withCommand("postgres", "-c", "fsync=on");

    private HikariDataSource pool;
    private JdbcTemplate jdbc;

    private ActorManager manager;
    private ActorReference<Tweet> actorRef;

    private TweetTransactionalOperations actorOperations;
    private TweetTransactionalOperations directOperations;

    @BeforeEach
    void setUp() {
        pool = newPool();
        jdbc = new JdbcTemplate(pool);

        // Separate tables to guarantee zero lock/resource contention between runs.
        actorOperations = new TweetTransactionalOperations(pool, "tweet_actor");
        directOperations = new TweetTransactionalOperations(pool, "tweet_direct");

        actorOperations.createTablePostgres();
        directOperations.createTablePostgres();

        PersistenceConfig<Tweet> config = new PersistenceConfig<>(
                actorOperations,
                1_000, // batchSize
                10L,   // timeoutMs
                3      // maxRetries
        );
        TweeterActor actor = new TweeterActor("tweeter-bench-" + UUID.randomUUID(), config);
        manager = new ActorManager();
        actorRef = manager.start(actor);
    }

    @AfterEach
    void tearDown() {
        if (actorRef != null) {
            actorRef.stop();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("both approaches persist every tweet; commit/WAL cost and throughput are reported")
    void benchmark_actorBatching_vs_perRowInserts() throws Exception {
        // ===================================================================
        // 1. WARMUP PHASE (both approaches), then start the measured run clean
        // ===================================================================
        warmupActor(WARMUP_TWEETS);
        warmupDirect(WARMUP_TWEETS);

        actorOperations.truncateTable();
        directOperations.truncateTable();

        assertEquals(0, actorOperations.countTweets(), "tweet_actor must be empty after warmup cleanup");
        assertEquals(0, directOperations.countTweets(), "tweet_direct must be empty after warmup cleanup");

        // ===================================================================
        // 2. BENCHMARK: Actor with natural batching
        //    VIRTUAL_THREADS submit tell() concurrently to the single actor
        // ===================================================================
        CountDownLatch actorSendGate = new CountDownLatch(1);
        CountDownLatch actorSendDone = new CountDownLatch(VIRTUAL_THREADS);

        PgStats actorBefore = PgStats.capture(jdbc, DB_NAME);
        long actorStartTime = System.nanoTime();

        for (int t = 0; t < VIRTUAL_THREADS; t++) {
            final int threadIndex = t;
            final int perThread = getPerThreadCount(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);
            final int offset = getOffset(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);

            Thread.ofVirtual().start(() -> {
                try {
                    actorSendGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perThread; i++) {
                    int id = offset + i;
                    actorRef.tell(new Tweet("actor-tw-" + id, "Actor benchmark tweet #" + id));
                }
                actorSendDone.countDown();
            });
        }

        actorSendGate.countDown();
        boolean actorSendFinished = actorSendDone.await(30, TimeUnit.SECONDS);
        assertTrue(actorSendFinished, "Actor sender threads timed out");

        // Wait until all tweets are committed in the database.
        await().pollInterval(Duration.ofMillis(5))
               .atMost(Duration.ofSeconds(60))
               .untilAsserted(() -> assertEquals(TOTAL_TWEETS, actorOperations.countTweets()));

        long actorEndTime = System.nanoTime();
        PgStats actorDelta = PgStats.capture(jdbc, DB_NAME).minus(actorBefore);
        double actorDurationMs = (actorEndTime - actorStartTime) / 1_000_000.0;
        double actorThroughput = (TOTAL_TWEETS / (actorDurationMs / 1000.0));

        // ===================================================================
        // 3. BENCHMARK: VIRTUAL_THREADS in parallel, one insert per row
        //    Each tweet is executed individually with JdbcTemplate.update()
        // ===================================================================
        CountDownLatch directGate = new CountDownLatch(1);
        CountDownLatch directDone = new CountDownLatch(VIRTUAL_THREADS);

        PgStats directBefore = PgStats.capture(jdbc, DB_NAME);
        long directStartTime = System.nanoTime();

        for (int t = 0; t < VIRTUAL_THREADS; t++) {
            final int threadIndex = t;
            final int perThread = getPerThreadCount(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);
            final int offset = getOffset(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);

            Thread.ofVirtual().start(() -> {
                try {
                    directGate.await();
                    // one single-insert statement (its own transaction) per row
                    for (int i = 0; i < perThread; i++) {
                        int id = offset + i;
                        directOperations.getJdbcTemplate().update(
                                "INSERT INTO tweet_direct (id, text) VALUES (?, ?)",
                                "direct-tw-" + id,
                                "Direct benchmark tweet #" + id
                        );
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    directDone.countDown();
                }
            });
        }

        directGate.countDown();
        boolean directFinished = directDone.await(120, TimeUnit.SECONDS);
        assertTrue(directFinished, "Direct virtual threads timed out");

        long directEndTime = System.nanoTime();
        PgStats directDelta = PgStats.capture(jdbc, DB_NAME).minus(directBefore);
        double directDurationMs = (directEndTime - directStartTime) / 1_000_000.0;
        double directThroughput = (TOTAL_TWEETS / (directDurationMs / 1000.0));

        assertEquals(TOTAL_TWEETS, directOperations.countTweets(),
                "Direct threads must insert all " + TOTAL_TWEETS + " tweets");

        // ===================================================================
        // 4. REPORT
        // ===================================================================
        double speedup = directDurationMs / actorDurationMs;
        double commitReduction = directDelta.xactCommit() / (double) Math.max(1, actorDelta.xactCommit());
        double walReduction = directDelta.walBytes() / (double) Math.max(1, actorDelta.walBytes());

        System.out.println();
        System.out.println("==========================================================================================");
        System.out.printf ("PERSISTENCE ACTOR BENCHMARK (Postgres)  —  %d tweets%n", TOTAL_TWEETS);
        System.out.printf ("Warmup: %d each  |  Parallel clients: %d virtual threads%n", WARMUP_TWEETS, VIRTUAL_THREADS);
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf ("%-34s %16s %16s%n", "", "ACTOR (batching)", "DIRECT (per-row)");
        System.out.printf ("%-34s %14.2f ms %14.2f ms%n", "end-to-end time", actorDurationMs, directDurationMs);
        System.out.printf ("%-34s %13.0f/s %13.0f/s%n", "throughput (tweets/s)", actorThroughput, directThroughput);
        System.out.printf ("%-34s %16d %16d%n", "pg xact_commit (delta)", actorDelta.xactCommit(), directDelta.xactCommit());
        System.out.printf ("%-34s %16d %16d%n", "pg wal_records (delta)", actorDelta.walRecords(), directDelta.walRecords());
        System.out.printf ("%-34s %16d %16d%n", "pg wal_bytes (delta)", actorDelta.walBytes(), directDelta.walBytes());
        System.out.printf ("%-34s %10d batches %10s%n", "actor bulk transactions",
                actorOperations.getRunCallCount(), TOTAL_TWEETS + " inserts");
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf ("Actor speedup:                 %6.2fx   |   commit reduction: %6.2fx   |   WAL reduction: %6.2fx%n",
                speedup, commitReduction, walReduction);
        System.out.println("==========================================================================================");
        System.out.println();

        // The whole thesis: the actor collapses N tiny commits into a handful of
        // fat ones, so it issues dramatically fewer commits than the per-row path.
        assertTrue(actorDelta.xactCommit() < directDelta.xactCommit(),
                "actor natural batching must issue far fewer commits than one-transaction-per-row");
    }

    // -----------------------------------------------------------------------
    // Infrastructure
    // -----------------------------------------------------------------------

    /**
     * The <b>shared</b> connection pool both approaches write through, so the
     * benchmark measures how each <em>drives</em> the database rather than the
     * cost of opening connections.
     */
    private HikariDataSource newPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(DB_POOL_SIZE);
        config.setMinimumIdle(DB_POOL_SIZE);
        config.setPoolName("bench-core-pg-pool");
        config.setAutoCommit(true);
        return new HikariDataSource(config);
    }

    // -----------------------------------------------------------------------
    // Warmup helpers
    // -----------------------------------------------------------------------

    private void warmupActor(int count) {
        for (int i = 0; i < count; i++) {
            actorRef.tell(new Tweet("warmup-act-" + i, "Warmup actor tweet " + i));
        }
        await().pollInterval(Duration.ofMillis(5))
               .atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(count, actorOperations.countTweets()));
    }

    private void warmupDirect(int count) throws Exception {
        CountDownLatch done = new CountDownLatch(VIRTUAL_THREADS);
        for (int t = 0; t < VIRTUAL_THREADS; t++) {
            final int threadIndex = t;
            final int perThread = getPerThreadCount(count, VIRTUAL_THREADS, threadIndex);
            final int offset = getOffset(count, VIRTUAL_THREADS, threadIndex);

            Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        int id = offset + i;
                        directOperations.getJdbcTemplate().update(
                                "INSERT INTO tweet_direct (id, text) VALUES (?, ?)",
                                "warmup-dir-" + id,
                                "Warmup direct tweet " + id
                        );
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }
        boolean finished = done.await(30, TimeUnit.SECONDS);
        assertTrue(finished, "Warmup direct threads timed out");
        assertEquals(count, directOperations.countTweets());
    }

    private static int getPerThreadCount(int total, int threads, int index) {
        int base = total / threads;
        int remainder = total % threads;
        return index < remainder ? base + 1 : base;
    }

    private static int getOffset(int total, int threads, int index) {
        int base = total / threads;
        int remainder = total % threads;
        if (index < remainder) {
            return index * (base + 1);
        } else {
            return remainder * (base + 1) + (index - remainder) * base;
        }
    }
}
