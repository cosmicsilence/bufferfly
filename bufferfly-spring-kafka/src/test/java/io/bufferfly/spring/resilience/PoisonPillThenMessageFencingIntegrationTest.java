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
import java.util.List;
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
 * and {@link ActorManager}, without a Kafka broker, so the <b>halt-at-pill</b>
 * contract can be pinned down deterministically.
 *
 * <p>The scenario: a poison pill sits <b>in the middle</b> of the stream,
 * immediately followed by a good message.
 *
 * <ol>
 *   <li>The real {@link NaturalBatchingDispatcher} coalesces the records and
 *       hands a batch to the real {@code BatchingPersistenceRunner}.</li>
 *   <li>{@link MessageOperations} (the real JDBC fence) rejects the oversized
 *       body with a {@code DataIntegrityViolationException}, so the runner
 *       isolates item-by-item: the good record <em>before</em> the pill persists,
 *       the pill is dead-lettered via {@code onPoisonPill} (which <b>advances the
 *       stored fencing offset past the pill</b>), and then the runner <b>halts</b>.</li>
 *   <li>The good record <em>after</em> the pill is therefore <b>not</b> persisted
 *       by this runner instance; the unrecoverable/restart hook fires. In a real
 *       Kafka deployment the actor restarts and seeks to the committed offset
 *       (now past the pill), re-consuming and persisting the trailing record from
 *       a clean state — the store never gets ahead of the rejected record.</li>
 * </ol>
 *
 * The assertions below prove exactly that boundary: the pre-pill record persists,
 * the pill is dead-lettered once and the fence advances past it, the post-pill
 * record is abandoned (not written by the halted runner), and the restart hook is
 * invoked. This {@link KafkaContext} is a no-op counter (no real broker), so the
 * trailing record's eventual re-processing is the restart machinery's job,
 * verified elsewhere; here we pin the <em>halt</em> itself.
 */
@Timeout(60)
@DisplayName("🦋 natural-batching × JDBC fencing token: a poison pill halts the batch; the record after it is abandoned, not persisted")
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
    void should_persistPrePillAndFencePastPill_but_abandonPostPill_andInvokeRestart() {
        final String oversized = "x".repeat(MessageOperations.BODY_MAX_LENGTH + 50);

        // Real natural-batching dispatcher + real token-backed JDBC operations,
        // wired through the real runner by PersistenceConfig. A roomy batch window
        // lets the three records coalesce so the pill is isolated with a good
        // record both before and after it in the same item-by-item pass.
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

        // ...the restart hook (onUnrecoverable) fires because the runner halts at
        // the pill. In production this is what restarts the actor and rewinds.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(1, restarts.get(),
                        "halting at the pill must invoke the restart hook exactly once"));

        // Only the record BEFORE the pill persists. The record after it is
        // abandoned by the halted runner (no real restart/replay in this harness).
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(1, operations.countForPartition(PARTITION),
                        "only the good record before the poison pill persists; the one after it is abandoned"));

        // The stored fencing offset advanced PAST the pill (to the pill's offset 1),
        // so a real restart would resume AFTER the pill — never re-delivering it.
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(1L, operations.loadStoredOffset(TOPIC, PARTITION),
                        "the fencing token must have advanced past the pill (to offset 1)"));

        // Hold briefly to prove it settles: no late insert of the abandoned record,
        // no re-delivery loop of the pill, no extra restarts.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8))
                .untilAsserted(() -> {
                    assertEquals(1, operations.countForPartition(PARTITION), "no late insert of the abandoned record");
                    assertEquals(1, operations.poisonPillCount(), "no re-delivery loop of the pill");
                    assertEquals(1, restarts.get(), "exactly one restart");
                });

        assertEquals(1L, operations.loadStoredOffset(TOPIC, PARTITION), "final stored offset is 1 (past the pill)");
        assertTrue(operations.bodiesForPartition(PARTITION).stream()
                        .noneMatch(b -> b.length() > MessageOperations.BODY_MAX_LENGTH),
                "the oversized body must never reach the DB");
        assertEquals(List.of("before-the-pill"), operations.bodiesForPartition(PARTITION),
                "only the pre-pill record is stored");
    }
}
