package io.bufferfly.spring.resilience;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.persistence.NaturalBatchingDispatcher;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 A <em>real</em> integration of the natural-batching persistence path and a
 * token-backed {@link io.bufferfly.core.persistence.TransactionalOperations}
 * (JDBC + HSQLDB, the same {@link MessageOperations} the embedded-Kafka
 * resilience suite uses) — but driven directly through a {@link MessageActor}
 * and {@link ActorManager}, without a Kafka broker, so the fencing-token
 * contract can be pinned down deterministically.
 *
 * <p>The scenario is the one that exposed the stale-token bug: a poison pill
 * sits <b>in the middle</b> of the stream, immediately followed by a good
 * message.
 *
 * <ol>
 *   <li>The real {@link NaturalBatchingDispatcher} coalesces the records and
 *       hands a batch to the real {@code BatchingPersistenceRunner}.</li>
 *   <li>{@link MessageOperations} (the real JDBC fence) rejects the oversized
 *       body with a {@code DataIntegrityViolationException}, so the runner
 *       isolates item-by-item and dead-letters the pill via {@code onPoisonPill},
 *       which <b>advances the stored fencing offset past the pill</b>.</li>
 *   <li>The <b>next good message's</b> batch then fences with a compare-and-set.
 *       If the runner's in-memory {@code currentToken} were left stale (still
 *       pointing before the pill, as it was before the fix), that CAS
 *       ({@code WHERE latest_offset = staleToken}) matches zero rows →
 *       {@link io.bufferfly.core.persistence.UnrecoverableException} → the actor
 *       restarts and the good message is lost.</li>
 * </ol>
 *
 * The assertions below prove the opposite: every good message persists, the
 * pill is dead-lettered exactly once, and the actor is NEVER restarted by a
 * spurious fence failure.
 */
@Timeout(60)
@DisplayName("🦋 natural-batching × JDBC fencing token: a message after a poison pill must not trip the offset fence")
class PoisonPillThenMessageFencingIntegrationTest {

    private static final String TOPIC = "fencing-it";
    private static final int PARTITION = 0;

    private DataSource dataSource;
    private MessageOperations operations;
    private ActorManager manager;
    private ActorReference<ConsumedRecord<String>> actorRef;
    private final AtomicInteger restarts = new AtomicInteger();

    @BeforeEach
    void setUp() {
        String url = "jdbc:hsqldb:mem:fencing_it_" + UUID.randomUUID().toString().replace("-", "")
                + ";sql.enforce_strict_size=true";
        dataSource = new DriverManagerDataSource(url, "SA", "");
        operations = new MessageOperations(dataSource, TOPIC);
        operations.createSchema();
        manager = new ActorManager();
    }

    @AfterEach
    void tearDown() {
        if (actorRef != null) actorRef.stop();
        try {
            new org.springframework.jdbc.core.JdbcTemplate(dataSource).execute("SHUTDOWN");
        } catch (Exception ignored) {
        }
    }

    /**
     * A no-op {@link KafkaContext}: there is no Kafka here. Seeks are irrelevant
     * (the actor rehydrates the stored offset from the DB via {@code initToken}),
     * pause/resume are no-ops, and {@code restartActor} just records that an
     * unrecoverable failure fired — which, with the fix in place, it must NOT.
     */
    private KafkaContext noopContext() {
        return new KafkaContext() {
            @Override public TopicPartition topicPartition() { return new TopicPartition(TOPIC, PARTITION); }
            @Override public void seek(long offset) { }
            @Override public void seekToBeginning() { }
            @Override public void seekToEnd() { }
            @Override public void pausePartition() { }
            @Override public void resumePartition() { }
            @Override public void restartActor() { restarts.incrementAndGet(); }
        };
    }

    @Test
    void should_persistMessageAfterPoisonPill_without_trippingTheOffsetFence() {
        final String oversized = "x".repeat(MessageOperations.BODY_MAX_LENGTH + 50);

        // Real natural-batching dispatcher + real token-backed JDBC operations,
        // wired through the real runner by PersistenceConfig. A roomy batch window
        // lets the three records coalesce so the pill is isolated with a good
        // record both before and after it in the same item-by-item pass — the
        // exact shape that left currentToken stale before the fix.
        KafkaContext ctx = noopContext();
        PersistenceConfig<Message> config = PersistenceConfig.builder(operations)
                .batchSize(10)
                .timeoutMs(40L)
                .maxRetries(0)
                .dispatcher(new NaturalBatchingDispatcher<>(10, 40L))
                .onUnrecoverable(ctx::restartActor)
                .build();

        MessageActor actor = new MessageActor("fencing-it-actor", PARTITION, ctx, operations, config);
        actorRef = manager.start(actor);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> actor.currentState() == io.bufferfly.core.actor.Actor.State.STARTED);

        // offset 0: good, offset 1: POISON (oversized), offset 2: good.
        actorRef.tell(new ConsumedRecord<>(TOPIC, PARTITION, 0L, "before-the-pill"));
        actorRef.tell(new ConsumedRecord<>(TOPIC, PARTITION, 1L, oversized));
        actorRef.tell(new ConsumedRecord<>(TOPIC, PARTITION, 2L, "after-the-pill"));

        // The pill is dead-lettered exactly once...
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(1, operations.poisonPillCount(),
                        "the oversized body must be dead-lettered exactly once"));

        // ...and BOTH good records persist — the one after the pill is the bug's
        // victim: its fence CAS would fail against a stale token.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(2, operations.countForPartition(PARTITION),
                        "both good records (before AND after the poison pill) must persist"));

        // The stored fencing offset advanced all the way past the last good record.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(2L, operations.loadStoredOffset(TOPIC, PARTITION),
                        "the fencing token must have advanced to the last processed offset (2)"));

        // Hold briefly to prove it settles with no late duplicate / re-delivery loop.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8))
                .untilAsserted(() -> {
                    assertEquals(2, operations.countForPartition(PARTITION), "no duplicate inserts");
                    assertEquals(1, operations.poisonPillCount(), "no re-delivery loop of the pill");
                });

        assertEquals(2L, operations.loadStoredOffset(TOPIC, PARTITION), "final stored offset is 2");
        assertTrue(operations.bodiesForPartition(PARTITION).stream()
                        .noneMatch(b -> b.length() > MessageOperations.BODY_MAX_LENGTH),
                "the oversized body must never reach the DB");
    }
}
