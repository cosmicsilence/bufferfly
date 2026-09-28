package io.bufferfly.core.persistence;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import org.hsqldb.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Separated resilience integration test verifying {@link AbstractPersistentActor} and {@link TweeterActor}
 * behavior when the underlying database becomes unavailable.
 *
 * <p>Uses HyperSQL Database (HSQLDB) running over TCP and proxied by NetCrusher ({@link TcpCrusher}).
 * When the database becomes unavailable (simulated by closing the TCP crusher proxy):
 * <ol>
 *   <li>The database connection fails.</li>
 *   <li>{@link TweetTransactionalOperations} detects the network/connection failure and throws {@link TransientException}.</li>
 *   <li>{@link BatchingPersistenceRunner} catches {@link TransientException}, retries with exponential back-off.</li>
 *   <li>The crusher is reopened, restoring database availability.</li>
 *   <li>On the subsequent retry attempt, the batch commits successfully and all tweets are saved without loss.</li>
 * </ol>
 */
@Timeout(60)
class PersistenceActorDatabaseUnavailableTest {

    private int dbPort;
    private int crusherPort;
    private Server hsqlServer;
    private NioReactor reactor;
    private TcpCrusher crusher;
    private String jdbcUrl;
    private ActorManager manager;
    private TweetTransactionalOperations operations;
    private BatchingPersistenceRunner<Tweet> runner;
    private TweeterActor actor;
    private ActorReference<Tweet> actorRef;
    private AtomicBoolean unrecoverableHookCalled;

    @BeforeEach
    void setUp() throws Exception {
        dbPort = findFreePort();
        crusherPort = findFreePort();

        String dbName = "crusherdb_" + UUID.randomUUID().toString().replace("-", "");

        // Start HSQLDB TCP Server
        hsqlServer = new Server();
        hsqlServer.setDatabaseName(0, dbName);
        hsqlServer.setDatabasePath(0, "mem:" + dbName + ";sql.syntax_ora=true;sql.enforce_strict_size=true");
        hsqlServer.setPort(dbPort);
        hsqlServer.setSilent(true);
        hsqlServer.setTrace(false);
        hsqlServer.start();

        // Start NetCrusher TCP Proxy between crusherPort and dbPort
        reactor = new NioReactor();
        crusher = TcpCrusherBuilder.builder()
                .withReactor(reactor)
                .withBindAddress(new InetSocketAddress("127.0.0.1", crusherPort))
                .withConnectAddress(new InetSocketAddress("127.0.0.1", dbPort))
                .buildAndOpen();

        // Connect JDBC through the NetCrusher proxy port
        jdbcUrl = "jdbc:hsqldb:hsql://127.0.0.1:" + crusherPort + "/" + dbName;

        operations = new TweetTransactionalOperations(jdbcUrl, "SA", "");
        operations.createTableIfNotExists();

        unrecoverableHookCalled = new AtomicBoolean(false);

        // Configure runner with 8 retries to allow ample window for transient recovery
        BatchingPersistenceRunner.Config config = new BatchingPersistenceRunner.Config(
                100,  // batch size
                20L,  // 20ms timeout
                8     // max retries on TransientException
        );

        runner = new BatchingPersistenceRunner<>(operations, config, () -> unrecoverableHookCalled.set(true));
        actor = new TweeterActor("tweeter-resilience-" + UUID.randomUUID(), runner);
        manager = new ActorManager();
        actorRef = manager.start(actor);
    }

    @AfterEach
    void tearDown() {
        if (actorRef != null) {
            actorRef.stop();
        }
        if (crusher != null && crusher.isOpen()) {
            crusher.close();
        }
        if (reactor != null) {
            reactor.close();
        }
        if (hsqlServer != null) {
            hsqlServer.shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — Database unavailable via TcpCrusher -> TransientException -> Restored -> Success
    // -----------------------------------------------------------------------

    @Test
    void should_retryAndPersistTweets_when_databaseBecomesUnavailableAndRestores() {
        // 1. Initially database is up: send an initial tweet to confirm healthy baseline
        actorRef.tell(new Tweet("initial-1", "Baseline tweet before outage"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, operations.countTweets());
                   assertNotNull(operations.findTweetById("initial-1"));
               });

        // 2. Simulate database outage: Close the NetCrusher TCP proxy
        crusher.close();
        assertFalse(crusher.isOpen(), "Crusher should be closed (DB unavailable)");

        // 3. Send tweets while the database is unavailable
        actorRef.tell(new Tweet("outage-1", "Tweet during network failure 1"));
        actorRef.tell(new Tweet("outage-2", "Tweet during network failure 2"));

        // 4. Verify that TransientException was thrown due to database unavailability
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertTrue(operations.getTransientExceptionCount() > 0,
                           "TransientException must be thrown when DB is unavailable");
                   // While DB is down, tweets must not be committed yet
                   assertEquals(1, operations.getPersistedCount(),
                           "Outage tweets should not be persisted while DB is down");
               });

        // 5. Restore the database connection by reopening the crusher proxy
        crusher.open();
        assertTrue(crusher.isOpen(), "Crusher should be open (DB restored)");

        // 6. Assert that runner's retry loop succeeds and all outage tweets are persisted
        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   assertEquals(3, operations.countTweets(),
                           "All outage tweets should be committed once DB restored");
                   assertNotNull(operations.findTweetById("outage-1"));
                   assertNotNull(operations.findTweetById("outage-2"));
                   assertFalse(unrecoverableHookCalled.get(),
                           "onUnrecoverable hook should NOT be called since retries succeeded");
               });

        // 7. Verify subsequent tweets sent after restoration are also persisted normally
        actorRef.tell(new Tweet("post-outage", "Tweet after network restored"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(4, operations.countTweets());
                   assertNotNull(operations.findTweetById("post-outage"));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Outage during high volume load
    // -----------------------------------------------------------------------

    @Test
    void should_recoverAllTweetsWithoutLoss_when_transientOutageOccursDuringHighVolume() throws InterruptedException {
        final int PRODUCERS = 10;
        final int TWEETS_PER_PRODUCER = 30;
        final int TOTAL_TWEETS = PRODUCERS * TWEETS_PER_PRODUCER; // 300 tweets

        // Cut connection before launching burst
        crusher.close();

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(PRODUCERS);

        for (int p = 0; p < PRODUCERS; p++) {
            final int producerId = p;
            Thread.ofVirtual().start(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < TWEETS_PER_PRODUCER; i++) {
                    actorRef.tell(new Tweet("burst-p" + producerId + "-t" + i, "High volume text " + i));
                }
                doneLatch.countDown();
            });
        }

        startGate.countDown();
        boolean sendersFinished = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(sendersFinished, "Producers should finish sending");

        // Wait until at least one TransientException is recorded while crusher is closed
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertTrue(operations.getTransientExceptionCount() > 0));

        // Restore network connectivity
        crusher.open();

        // Await all tweets successfully committed to the database
        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> {
                   assertEquals(TOTAL_TWEETS, actor.getReceiveCount(), "Actor receive count mismatch");
                   assertEquals(TOTAL_TWEETS, operations.countTweets(), "All 300 tweets must be inserted into DB");
                   assertEquals(0, operations.getPoisonPills().size(), "No poison pills expected");
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 3 — Permanent outage exhausts retries -> unrecoverable hook invoked
    // -----------------------------------------------------------------------

    @Test
    void should_exhaustRetriesAndCallUnrecoverable_when_databaseNeverRestores() {
        // Fast retries configuration
        BatchingPersistenceRunner.Config fastConfig = new BatchingPersistenceRunner.Config(
                10,   // batch size
                10L,  // 10ms timeout
                2     // only 2 retries
        );

        AtomicBoolean unrecoverableTriggered = new AtomicBoolean(false);
        BatchingPersistenceRunner<Tweet> fastRunner = new BatchingPersistenceRunner<>(
                operations, fastConfig, () -> unrecoverableTriggered.set(true));

        TweeterActor exhaustActor = new TweeterActor("exhaust-actor", fastRunner);
        ActorReference<Tweet> exhaustRef = manager.start(exhaustActor);

        // Close crusher permanently
        crusher.close();

        // Send a tweet that cannot be saved
        exhaustRef.tell(new Tweet("permanent-fail", "Will never be persisted"));

        // Wait for retries to exhaust and onUnrecoverable to fire
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertTrue(operations.getTransientExceptionCount() >= 2,
                           "TransientException should be caught on retries");
                   assertTrue(unrecoverableTriggered.get(),
                           "onUnrecoverable hook must be invoked after exhausting retries");
               });

        exhaustRef.stop();
    }

    // -----------------------------------------------------------------------
    // Helper to find free port
    // -----------------------------------------------------------------------

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
