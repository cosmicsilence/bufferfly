package io.cosmicsilence.bufferfly.core.persistence;

import io.cosmicsilence.bufferfly.core.actor.ActorManager;
import io.cosmicsilence.bufferfly.core.actor.ActorReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark test comparing:
 * <ol>
 *   <li><b>Actor with natural batching</b>: {@link TweeterActor} enqueues messages via
 *       {@link BatchingPersistenceRunner}, which coalesces writes into natural bulk batches.</li>
 *   <li><b>15 parallel virtual threads without batching</b>: 15 virtual threads concurrently
 *       inserting tweets one-by-one via individual JDBC {@code executeUpdate()} calls without batching.</li>
 * </ol>
 *
 * <p>Scenario:
 * <ul>
 *   <li>10,000 tweets per scenario.</li>
 *   <li>Warmup phase executed for both approaches prior to measurement to ensure JIT compilation,
 *       connection warmup, and class loading do not skew results.</li>
 *   <li>Full verification that all 10,000 tweets are persisted in both scenarios.</li>
 * </ul>
 */
@Timeout(60)
class PersistenceActorBenchmarkTest {

    private static final int TOTAL_TWEETS = 100_000;
    private static final int WARMUP_TWEETS = 1_000;
    private static final int VIRTUAL_THREADS = 15;

    private String dbName;
    private String jdbcUrl;

    private ActorManager manager;
    private BatchingPersistenceRunner<Tweet> runner;
    private TweeterActor actor;
    private ActorReference<Tweet> actorRef;

    private TweetTransactionalOperations actorOperations;
    private TweetTransactionalOperations directOperations;

    @BeforeEach
    void setUp() throws SQLException {
        dbName = "benchdb_" + UUID.randomUUID().toString().replace("-", "");
        jdbcUrl = "jdbc:hsqldb:mem:" + dbName + ";sql.syntax_ora=true;sql.enforce_strict_size=true";

        // Separate tables to guarantee zero lock/resource contention between runs
        actorOperations = new TweetTransactionalOperations(jdbcUrl, "SA", "", "tweet_actor");
        directOperations = new TweetTransactionalOperations(jdbcUrl, "SA", "", "tweet_direct");

        actorOperations.createTableIfNotExists();
        directOperations.createTableIfNotExists();

        BatchingPersistenceRunner.Config config = new BatchingPersistenceRunner.Config(
                1_000, // batchSize
                10L,   // timeoutMs
                3      // maxRetries
        );
        runner = new BatchingPersistenceRunner<>(actorOperations, config);
        actor = new TweeterActor("tweeter-bench-" + UUID.randomUUID(), runner);
        manager = new ActorManager();
        actorRef = manager.start(actor);
    }

    @AfterEach
    void tearDown() {
        if (actorRef != null) {
            actorRef.stop();
        }
        try {
            actorOperations.getJdbcTemplate().execute("SHUTDOWN");
        } catch (Exception ignored) {
        }
    }

    @Test
    void benchmark_actorBatching_vs_15VirtualThreadsWithoutBatching() throws Exception {
        // ===================================================================
        // 1. WARMUP PHASE (Both approaches)
        // ===================================================================
        warmupActor(WARMUP_TWEETS);
        warmupDirect(WARMUP_TWEETS);

        // Truncate tables after warmup so measurements start from clean state
        actorOperations.truncateTable();
        directOperations.truncateTable();

        assertEquals(0, actorOperations.countTweets(), "tweet_actor must be empty after warmup cleanup");
        assertEquals(0, directOperations.countTweets(), "tweet_direct must be empty after warmup cleanup");

        // ===================================================================
        // 2. BENCHMARK: Actor with Natural Batching (10,000 tweets)
        //    15 virtual threads submit tell() concurrently to the actor
        // ===================================================================
        CountDownLatch actorSendGate = new CountDownLatch(1);
        CountDownLatch actorSendDone = new CountDownLatch(VIRTUAL_THREADS);

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
        boolean actorSendFinished = actorSendDone.await(15, TimeUnit.SECONDS);
        assertTrue(actorSendFinished, "Actor sender threads timed out");

        // Wait until all 10k tweets are committed in the database
        await().pollInterval(Duration.ofMillis(2))
               .atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(TOTAL_TWEETS, actorOperations.countTweets()));

        long actorEndTime = System.nanoTime();
        double actorDurationMs = (actorEndTime - actorStartTime) / 1_000_000.0;
        double actorThroughput = (TOTAL_TWEETS / (actorDurationMs / 1000.0));

        // ===================================================================
        // 3. BENCHMARK: 15 Virtual Threads in Parallel Without Batching (10,000 tweets)
        //    Each tweet is executed individually with JdbcTemplate.update()
        // ===================================================================
        CountDownLatch directGate = new CountDownLatch(1);
        CountDownLatch directDone = new CountDownLatch(VIRTUAL_THREADS);

        long directStartTime = System.nanoTime();

        for (int t = 0; t < VIRTUAL_THREADS; t++) {
            final int threadIndex = t;
            final int perThread = getPerThreadCount(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);
            final int offset = getOffset(TOTAL_TWEETS, VIRTUAL_THREADS, threadIndex);

            Thread.ofVirtual().start(() -> {
                try {
                    directGate.await();
                    // Each virtual thread executes individual single-insert statements without batching
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
        boolean directFinished = directDone.await(30, TimeUnit.SECONDS);
        assertTrue(directFinished, "Direct virtual threads timed out");

        long directEndTime = System.nanoTime();
        double directDurationMs = (directEndTime - directStartTime) / 1_000_000.0;
        double directThroughput = (TOTAL_TWEETS / (directDurationMs / 1000.0));

        // Assert all 10k tweets inserted
        assertEquals(TOTAL_TWEETS, directOperations.countTweets(),
                "Direct threads must insert all 10,000 tweets");

        // ===================================================================
        // 4. PRINT BENCHMARK COMPARISON
        // ===================================================================
        double speedup = directDurationMs / actorDurationMs;

        System.out.println();
        System.out.println("================================================================================");
        System.out.println("PERSISTENCE ACTOR BENCHMARK: " + TOTAL_TWEETS + " TWEETS");
        System.out.println("Warmup: " + WARMUP_TWEETS + " tweets for both | Parallel Clients: " + VIRTUAL_THREADS + " virtual threads");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("1. Actor with Natural Batching:          %8.2f ms  (%8.0f tweets/sec)%n",
                actorDurationMs, actorThroughput);
        System.out.printf("   Batches executed:                     %8d batches (avg %.1f tweets/batch)%n",
                actorOperations.getRunCallCount(),
                (double) TOTAL_TWEETS / Math.max(1, actorOperations.getRunCallCount()));
        System.out.printf("2. 15 Virtual Threads (No Batching):     %8.2f ms  (%8.0f tweets/sec)%n",
                directDurationMs, directThroughput);
        System.out.printf("   Single-item executeUpdate() calls:    %8d individual calls%n",
                TOTAL_TWEETS);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Speedup factor:                          %8.2fx faster with Actor batching%n",
                speedup);
        System.out.println("================================================================================");
        System.out.println();
    }

    // -----------------------------------------------------------------------
    // Warmup Helpers
    // -----------------------------------------------------------------------

    private void warmupActor(int count) {
        for (int i = 0; i < count; i++) {
            actorRef.tell(new Tweet("warmup-act-" + i, "Warmup actor tweet " + i));
        }
        await().pollInterval(Duration.ofMillis(2))
               .atMost(Duration.ofSeconds(10))
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
        boolean finished = done.await(10, TimeUnit.SECONDS);
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
