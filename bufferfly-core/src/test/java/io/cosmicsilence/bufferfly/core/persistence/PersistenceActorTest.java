package io.cosmicsilence.bufferfly.core.persistence;

import io.cosmicsilence.bufferfly.core.actor.ActorManager;
import io.cosmicsilence.bufferfly.core.actor.ActorReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and integration tests for {@link AbstractPersistentActor} and {@link TweeterActor}.
 *
 * <p>Uses an in-memory HSQLDB database with Oracle syntax and strict size enforcement:
 * {@code CREATE TABLE tweet (id VARCHAR(50) PRIMARY KEY, text VARCHAR2(100))}
 *
 * <p>Scenarios covered:
 * <ol>
 *   <li>Single tweet addition — successfully persisted to the database.</li>
 *   <li>Multiple tweets addition — all items persisted in order.</li>
 *   <li>Poison pill handling — a tweet with 101 characters causes a string right truncation error;
 *       the runner isolates the poison pill, calls {@code onPoisonPill}, persists the valid
 *       tweets in the batch, and remains healthy for subsequent messages.</li>
 *   <li>Single poison pill — isolated tweet of 101 chars handled gracefully.</li>
 *   <li>High volume burst — 2 000 tweets concurrently sent from 20 virtual threads; asserts
 *       all 2 000 are committed with zero message loss.</li>
 *   <li>Lifecycle — actor stopping closes the runner gracefully.</li>
 * </ol>
 */
@Timeout(60)
class PersistenceActorTest {

    private String dbName;
    private String jdbcUrl;
    private ActorManager manager;
    private TweetTransactionalOperations operations;
    private BatchingPersistenceRunner<Tweet> runner;
    private TweeterActor actor;
    private ActorReference<Tweet> actorRef;

    @BeforeEach
    void setUp() {
        dbName = "tweetdb_" + UUID.randomUUID().toString().replace("-", "");
        jdbcUrl = "jdbc:hsqldb:mem:" + dbName + ";sql.syntax_ora=true;sql.enforce_strict_size=true";

        operations = new TweetTransactionalOperations(jdbcUrl, "SA", "");
        operations.createTableIfNotExists();

        BatchingPersistenceRunner.Config config = new BatchingPersistenceRunner.Config(
                500,  // batch size
                20L,  // 20ms timeout
                3     // 3 retries
        );
        runner = new BatchingPersistenceRunner<>(operations, config);

        actor = new TweeterActor("tweeter-" + UUID.randomUUID(), runner);
        manager = new ActorManager();
        actorRef = manager.start(actor);
    }

    @AfterEach
    void tearDown() {
        if (actorRef != null) {
            actorRef.stop();
        }
        try {
            operations.getJdbcTemplate().execute("SHUTDOWN");
        } catch (Exception ignored) {
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — Simple adding single tweet
    // -----------------------------------------------------------------------

    @Test
    void should_insertSingleTweet_when_actorReceivesTweet() {
        Tweet tweet = new Tweet("tweet-1", "Hello Bufferfly!");
        actorRef.tell(tweet);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, actor.getReceiveCount());
                   assertEquals(1, operations.countTweets());
                   Tweet saved = operations.findTweetById("tweet-1");
                   assertNotNull(saved);
                   assertEquals("Hello Bufferfly!", saved.text());
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Multiple tweets adding
    // -----------------------------------------------------------------------

    @Test
    void should_insertMultipleTweets_when_actorReceivesMultipleTweets() {
        actorRef.tell(new Tweet("tweet-1", "First tweet"));
        actorRef.tell(new Tweet("tweet-2", "Second tweet"));
        actorRef.tell(new Tweet("tweet-3", "Third tweet"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(3, actor.getReceiveCount());
                   assertEquals(3, operations.countTweets());
                   assertNotNull(operations.findTweetById("tweet-1"));
                   assertNotNull(operations.findTweetById("tweet-2"));
                   assertNotNull(operations.findTweetById("tweet-3"));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 3 — Poison pill: 101 characters in batch
    // -----------------------------------------------------------------------

    @Test
    void should_isolatePoisonPillAndInsertValidTweets_when_tweetExceedsVarcharLength() {
        // tweet table has TEXT VARCHAR2(100).
        // 100 characters must succeed.
        String exactly100 = "A".repeat(100);
        // 101 characters is the poison pill.
        String poisonPill101 = "B".repeat(101);

        Tweet valid1 = new Tweet("valid-1", "Valid start tweet");
        Tweet poison = new Tweet("poison-1", poisonPill101);
        Tweet valid2 = new Tweet("valid-2", exactly100);
        Tweet valid3 = new Tweet("valid-3", "Valid end tweet");

        // Send all tweets in quick succession so they form a batch
        actorRef.tell(valid1);
        actorRef.tell(poison);
        actorRef.tell(valid2);
        actorRef.tell(valid3);

        // The poison pill must be isolated to onPoisonPill, while valid tweets are persisted
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(4, actor.getReceiveCount());
                   // Valid tweets (valid1, valid2, valid3) must be in the DB
                   assertEquals(3, operations.countTweets(), "Only the 3 valid tweets should be in DB");
                   assertNotNull(operations.findTweetById("valid-1"));
                   assertNotNull(operations.findTweetById("valid-2"));
                   assertNotNull(operations.findTweetById("valid-3"));
                   assertNull(operations.findTweetById("poison-1"));

                   // Poison pill recorded
                   assertEquals(1, operations.getPoisonPills().size(), "Poison pill count mismatch");
                   assertEquals("poison-1", operations.getPoisonPills().get(0).id());
                   assertEquals(101, operations.getPoisonPills().get(0).text().length());
                   assertFalse(operations.getPoisonPillCauses().isEmpty());
               });

        // Verify the actor and persistence runner continue operating normally after the poison pill
        actorRef.tell(new Tweet("subsequent", "I am alive after the poison pill"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(4, operations.countTweets());
                   assertNotNull(operations.findTweetById("subsequent"));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 4 — Poison pill sent alone
    // -----------------------------------------------------------------------

    @Test
    void should_handlePoisonPillGracefully_when_poisonPillSentAlone() {
        String poisonPill101 = "P".repeat(101);
        Tweet poison = new Tweet("alone-poison", poisonPill101);

        actorRef.tell(poison);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, actor.getReceiveCount());
                   assertEquals(0, operations.countTweets());
                   assertEquals(1, operations.getPoisonPills().size());
                   assertEquals("alone-poison", operations.getPoisonPills().get(0).id());
               });

        // Verify actor can still accept and persist good messages
        actorRef.tell(new Tweet("recovery", "Working after standalone poison pill"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, operations.countTweets());
                   assertNotNull(operations.findTweetById("recovery"));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 5 — High volume scenario
    // -----------------------------------------------------------------------

    @Test
    void should_insertAllTweets_when_highVolumeBurstReceived() throws InterruptedException {
        final int PRODUCER_THREADS = 20;
        final int TWEETS_PER_PRODUCER = 100;
        final int TOTAL_TWEETS = PRODUCER_THREADS * TWEETS_PER_PRODUCER; // 2 000 tweets

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(PRODUCER_THREADS);

        for (int p = 0; p < PRODUCER_THREADS; p++) {
            final int producerId = p;
            Thread.ofVirtual().start(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < TWEETS_PER_PRODUCER; i++) {
                    String id = "p" + producerId + "-tw" + i;
                    String text = "High volume tweet #" + i + " from producer " + producerId;
                    actorRef.tell(new Tweet(id, text));
                }
                doneLatch.countDown();
            });
        }

        startGate.countDown();
        boolean finished = doneLatch.await(15, TimeUnit.SECONDS);
        assertTrue(finished, "Senders did not complete in time");

        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> {
                   assertEquals(TOTAL_TWEETS, actor.getReceiveCount(), "Actor receive count mismatch");
                   assertEquals(TOTAL_TWEETS, operations.countTweets(), "DB tweet count mismatch");
                   assertEquals(0, operations.getPoisonPills().size(), "There should be no poison pills");
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 6 — Stop lifecycle stops the runner
    // -----------------------------------------------------------------------

    @Test
    void should_stopRunner_when_actorIsStopped() {
        actorRef.tell(new Tweet("life-1", "Lifecycle test"));

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, operations.countTweets()));

        actorRef.stop();

        assertFalse(actor.isStarted(), "Actor should be marked stopped");
        // Verify runner throws IllegalStateException if new items are enqueued after stop
        assertThrows(IllegalStateException.class, () -> runner.enqueue(new Tweet("illegal", "test")));
    }
}
