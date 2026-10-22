package io.bufferfly.spring.resilience;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.spring.ShardedActorContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 Resilience suite for sharded, persistent actors over embedded Kafka. Three
 * independent actors (one per partition of a 3-partition topic) each store a
 * string and stamp it with a per-partition auto-incremented id. Each scenario
 * stresses a different failure mode and asserts the shock-absorber guarantees:
 *
 * <ol>
 *   <li><b>Backpressure isolation</b> — throttling <em>partition 0</em>'s DB
 *       writes engages backpressure and pauses <em>only</em> partition 0, while
 *       partitions 1 and 2 keep persisting; when the slow writer catches up,
 *       partition 0 resumes and loses nothing.</li>
 *   <li><b>Poison-pill isolation</b> — a message too large for the DB column is
 *       dead-lettered (to an in-memory list) every time, on one partition only;
 *       its good neighbours still persist and the other partitions are untouched.</li>
 *   <li><b>Rebalance / failover</b> — three single-consumer containers share the
 *       group; stopping one moves its partition to a surviving consumer, whose
 *       fresh actor refreshes its id sequence + offset from the DB and continues
 *       with no message loss and no duplicates.</li>
 * </ol>
 */
@SpringJUnitConfig(ShardedActorResilienceTest.EmptyConfig.class)
@EmbeddedKafka(partitions = 3, topics = {ShardedActorResilienceTest.TOPIC})
@Timeout(180)
@DisplayName("🦋 Sharded actor resilience: backpressure, poison-pill, and failover isolation")
class ShardedActorResilienceTest {

    static final String TOPIC = "bufferfly-resilience-events";
    static final String GROUP = "resilience-group";

    static class EmptyConfig {
    }

    @Autowired
    private EmbeddedKafkaBroker broker;

    private DataSource dataSource;
    private MessageOperations operations;
    private final List<ShardedActorContainer<String, String>> containers = new CopyOnWriteArrayList<>();
    private KafkaTemplate<String, String> producer;

    /** A fresh topic + group per test so no records bleed across methods. */
    private String topic;
    private String group;

    @BeforeEach
    void setUp() {
        // Unique topic & group per test: the embedded broker is shared across the
        // class, so reusing one topic would let a prior test's records be
        // re-consumed (fresh DB => actors seek to beginning), inflating counts.
        // The topic itself is created per-test via createTopic(partitions), so
        // each test picks its own partition count.
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        topic = TOPIC + "-" + suffix;
        group = GROUP + "-" + suffix;

        String url = "jdbc:hsqldb:mem:resilience_" + UUID.randomUUID().toString().replace("-", "")
                + ";sql.enforce_strict_size=true";
        dataSource = new DriverManagerDataSource(url, "SA", "");
        operations = new MessageOperations(dataSource, topic);
        operations.createSchema();

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        producer = new KafkaTemplate<>(pf);
    }

    /** Creates this test's topic with {@code partitions} partitions on the embedded broker. */
    private void createTopic(int partitions) {
        broker.addTopics(new org.apache.kafka.clients.admin.NewTopic(topic, partitions, (short) 1));
    }

    @AfterEach
    void tearDown() {
        containers.forEach(c -> {
            try {
                c.stop();
            } catch (Exception ignored) {
            }
        });
        containers.clear();
        if (producer != null) producer.destroy();
    }

    // -- shared helpers -----------------------------------------------------

    private DefaultKafkaConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, group, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private ShardedActorContainer<String, String> startContainer(int concurrency, MessageShardFactory factory) {
        ShardedActorContainer<String, String> c = new ShardedActorContainer<>(
                group, topic, concurrency, factory, consumerFactory(), new ActorManager());
        containers.add(c);
        c.start();
        return c;
    }

    /** Publish {@code count} records to a specific partition. */
    private void produceTo(int partition, int count, String prefix) {
        for (int i = 0; i < count; i++) {
            producer.send(topic, partition, "k" + partition, prefix + "-p" + partition + "-m" + i);
        }
        producer.flush();
    }

    // ======================================================================
    //  TEST 1 — backpressure isolation
    // ======================================================================

    @Test
    @DisplayName("backpressure on partition 0 pauses only it; partitions 1 & 2 keep storing; no loss")
    void backpressure_on_one_partition_does_not_stall_the_others() {
        final int perPartition = 300;
        createTopic(3);

        // Throttle ONLY partition 0 so its buffer backs up and engages backpressure.
        operations.withPersistDelay(0, 40L);

        // Small watermarks + tiny evaluate window so partition 0 pauses quickly and
        // resumes reliably once drained. Partitions 1 & 2 (no delay) never saturate.
        MessageShardFactory factory = new MessageShardFactory(
                operations, 5, 5L, 10, 2, 5L);

        ShardedActorContainer<String, String> container = startContainer(3, factory);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(3, container.shardCount(), "one actor per partition"));

        // Flood all three partitions at once.
        produceTo(0, perPartition, "bp");
        produceTo(1, perPartition, "bp");
        produceTo(2, perPartition, "bp");

        // --- partition 0 must engage backpressure (pause) ---
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertTrue(factory.pauseCount(0) >= 1,
                        "the throttled partition 0 must engage backpressure and pause"));

        // --- while partition 0 is throttled, partitions 1 & 2 keep storing and
        //     finish well ahead of it (they are not stalled by partition 0) ---
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertEquals(perPartition, operations.countForPartition(1),
                    "partition 1 must keep persisting while partition 0 is paused");
            assertEquals(perPartition, operations.countForPartition(2),
                    "partition 2 must keep persisting while partition 0 is paused");
        });
        // the other partitions were never paused
        assertEquals(0, factory.pauseCount(1), "partition 1 must not be paused");
        assertEquals(0, factory.pauseCount(2), "partition 2 must not be paused");

        // --- eventually the throttled partition catches up: resume fires, nothing
        //     is left paused, and every record is persisted exactly once ---
        await().atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> assertEquals(perPartition, operations.countForPartition(0),
                        "every record on the throttled partition must persist (pause != drop)"));
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertTrue(factory.resumeCount(0) >= 1,
                        "partition 0 must resume once its buffer drains"));
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertFalse(factory.isPartitionPaused(0),
                        "no partition may be left paused at the end"));

        assertEquals(3 * perPartition, operations.totalCount(), "no message lost across partitions");
        // ids are a contiguous 1..N per partition (auto-increment held under pressure)
        assertEquals(contiguous(perPartition), operations.idsForPartition(0),
                "partition 0 ids must be a contiguous 1..N despite the pause/resume");
    }

    // ======================================================================
    //  TEST 2 — poison-pill isolation (single partition targeted)
    // ======================================================================

    @Test
    @DisplayName("an always-too-large message on partition 1 is dead-lettered every time; others store fine")
    void poison_pill_is_deadlettered_and_other_partitions_keep_running() {
        // A body longer than the DB column: can NEVER be persisted -> poison pill.
        final String oversized = "x".repeat(MessageOperations.BODY_MAX_LENGTH + 50);
        final int targetPartition = 1;
        createTopic(3);

        MessageShardFactory factory = new MessageShardFactory(
                operations, 5, 5L, 10_000, 5_000, 25L); // watermarks high: no backpressure here

        ShardedActorContainer<String, String> container = startContainer(3, factory);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(3, container.shardCount(), "one actor per partition"));

        // Keep partitions 0 and 2 busy with good traffic the whole time.
        produceTo(0, 50, "good");
        produceTo(2, 50, "good");

        // On the TARGET partition only: some good messages, then the poison pill,
        // then more good messages. The pill is isolated and dead-lettered; the
        // surrounding good messages persist.
        produceTo(targetPartition, 5, "before");
        producer.send(topic, targetPartition, "k" + targetPartition, oversized);
        producer.flush();
        produceTo(targetPartition, 5, "after");

        // --- the poison pill is dead-lettered to the in-memory list (exactly once) ---
        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> assertEquals(1, operations.poisonPillCount(),
                        "the oversized message must be dead-lettered to the in-memory list"));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertEquals(1, operations.poisonPillCount(),
                        "it must be dead-lettered exactly once (no re-delivery loop)"));
        assertEquals(targetPartition, operations.poisonPills().get(0).partition(),
                "the dead-lettered pill came from the target partition");

        // --- the 10 good messages on the target partition persist (5 before + 5 after) ---
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(10, operations.countForPartition(targetPartition),
                        "every good message before and after the poison pill must persist"));

        // --- the OTHER partitions were untouched by the pill and stored everything ---
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertEquals(50, operations.countForPartition(0), "partition 0 unaffected by the pill");
            assertEquals(50, operations.countForPartition(2), "partition 2 unaffected by the pill");
        });

        // the oversized body is NOT in the DB; only good bodies are
        assertTrue(operations.bodiesForPartition(targetPartition).stream().noneMatch(b -> b.length() > MessageOperations.BODY_MAX_LENGTH),
                "the oversized body must never reach the DB");
        assertEquals(0, operations.poisonPillCount() - 1, "exactly one pill, nothing else dead-lettered");
    }

    // ======================================================================
    //  TEST 3 — rebalance / failover without message loss
    // ======================================================================

    @Test
    @DisplayName("stopping one consumer reassigns its partition; a fresh actor resumes from DB with no loss/dup")
    void stopping_a_consumer_failsover_its_partition_without_loss() {
        final int firstBatch = 40;
        final int secondBatch = 40;
        createTopic(3);

        MessageShardFactory factory = new MessageShardFactory(
                operations, 10, 5L, 10_000, 5_000, 25L); // no backpressure in this scenario

        // Three single-consumer containers in the same group -> one partition each.
        ShardedActorContainer<String, String> c0 = startContainer(1, factory);
        ShardedActorContainer<String, String> c1 = startContainer(1, factory);
        ShardedActorContainer<String, String> c2 = startContainer(1, factory);

        // all 3 partitions assigned across the 3 consumers
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            int shards = c0.shardCount() + c1.shardCount() + c2.shardCount();
            assertEquals(3, shards, "all 3 partitions assigned across the group");
        });

        // First wave to every partition; wait until all durably stored.
        for (int p = 0; p < 3; p++) produceTo(p, firstBatch, "wave1");
        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> assertEquals(3 * firstBatch, operations.totalCount(),
                        "first wave must be fully persisted before failover"));

        int actorsBeforeFailover = factory.createdActorCount();

        // --- FAILOVER: stop one consumer. Its partition(s) are revoked and
        //     reassigned to a surviving consumer, which stands up a fresh actor. ---
        c0.stop();
        containers.remove(c0);

        // the surviving consumers must take over all 3 partitions, and the factory
        // must have created at least one NEW actor for the reassigned partition(s)
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            int shards = c1.shardCount() + c2.shardCount();
            assertEquals(3, shards, "surviving consumers must own all 3 partitions after failover");
        });
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertTrue(factory.createdActorCount() > actorsBeforeFailover,
                        "a fresh actor must be created for the reassigned partition(s)"));

        // --- second wave AFTER failover to every partition ---
        for (int p = 0; p < 3; p++) produceTo(p, secondBatch, "wave2");

        // --- no loss, no duplicates: every produced record is stored exactly once ---
        await().atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> assertEquals(3 * (firstBatch + secondBatch), operations.totalCount(),
                        "every record across both waves must persist exactly once after failover"));
        // hold to prove it settles (the refreshed actor did not re-insert on rewind)
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertEquals(3 * (firstBatch + secondBatch), operations.totalCount(),
                        "no duplicate inserts after the reassigned actor rewound/refreshed"));

        // each partition holds exactly firstBatch+secondBatch, with a contiguous
        // 1..N id sequence (the failed-over actor continued the sequence from the DB)
        for (int p = 0; p < 3; p++) {
            assertEquals(firstBatch + secondBatch, operations.countForPartition(p),
                    "partition " + p + " must hold every record across the failover");
            assertEquals(contiguous(firstBatch + secondBatch), operations.idsForPartition(p),
                    "partition " + p + " ids must be a contiguous 1..N across the failover (no gaps/dups)");
        }
    }

    // ======================================================================
    //  TEST 4 — multiple partitions per consumer under concurrency > 1
    // ======================================================================

    @Test
    @DisplayName("concurrency>1 with more partitions than consumers: shared listener loses nothing under concurrent delivery")
    void concurrency_multiplePartitionsPerConsumer_noLossUnderConcurrentListenerThreads() {
        final int partitions = 6;
        final int concurrency = 2; // => each of 2 consumers owns ~3 partitions
        final int perPartition = 150;
        createTopic(partitions);

        // No throttle, high watermarks: we are stressing concurrent routing/persistence
        // through the single shared PartitionRoutingListener, not backpressure.
        MessageShardFactory factory = new MessageShardFactory(
                operations, 20, 5L, 100_000, 50_000, 25L);

        ShardedActorContainer<String, String> container = startContainer(concurrency, factory);

        // All 6 partitions must be assigned (across the 2 consumers), each with an actor.
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(partitions, container.shardCount(),
                        "all " + partitions + " partitions must be assigned across the " + concurrency + " consumers"));

        // Flood every partition: records arrive on the ONE shared listener from
        // BOTH consumer threads concurrently (the thread-safety case Spring warns about).
        for (int p = 0; p < partitions; p++) {
            produceTo(p, perPartition, "cc");
        }

        // --- no message loss across all partitions ---
        await().atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> assertEquals(partitions * perPartition, operations.totalCount(),
                        "every record on every partition must persist exactly once (no loss under concurrency)"));
        // settle to prove no late duplicates
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertEquals(partitions * perPartition, operations.totalCount(),
                        "record count must settle (no duplicates under concurrent delivery)"));

        // --- per-partition isolation + contiguous ids: proves records were routed
        //     to the correct per-partition actor despite the shared listener ---
        for (int p = 0; p < partitions; p++) {
            assertEquals(perPartition, operations.countForPartition(p),
                    "partition " + p + " must hold exactly its own records");
            assertEquals(contiguous(perPartition), operations.idsForPartition(p),
                    "partition " + p + " ids must be contiguous 1..N (correct single-writer per partition)");
        }
        assertEquals(0, operations.poisonPillCount(), "no poison pills in this run");
    }

    // ======================================================================
    //  TEST 5 — per-partition pause isolation WITHIN a single consumer
    // ======================================================================

    @Test
    @DisplayName("pausing one partition does not pause its co-located partition on the SAME consumer")
    void backpressure_pausesOnePartition_whileCoLocatedPartitionOnSameConsumerKeepsFlowing() {
        final int partitions = 2;
        final int perPartition = 300;
        createTopic(partitions);

        // ONE consumer (concurrency 1) owns BOTH partitions, so pausing partition 0
        // must stop fetching ONLY partition 0 while the same consumer keeps polling
        // (and this container keeps persisting) partition 1.
        operations.withPersistDelay(0, 40L); // throttle ONLY partition 0 -> it saturates & pauses

        MessageShardFactory factory = new MessageShardFactory(
                operations, 5, 5L, 10, 2, 5L); // small watermarks + tiny evaluate window

        ShardedActorContainer<String, String> container = startContainer(1, factory);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(partitions, container.shardCount(),
                        "the single consumer must own BOTH partitions"));

        produceTo(0, perPartition, "iso");
        produceTo(1, perPartition, "iso");

        // --- partition 0 engages backpressure (pauses) ---
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertTrue(factory.pauseCount(0) >= 1,
                        "throttled partition 0 must pause"));

        // --- CRITICAL: while partition 0 is paused, the CO-LOCATED partition 1 on
        //     the SAME consumer must keep flowing and FINISH (pausePartition(tp0)
        //     must not stall the whole consumer) ---
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(perPartition, operations.countForPartition(1),
                        "co-located partition 1 must keep persisting while partition 0 is paused on the same consumer"));
        assertEquals(0, factory.pauseCount(1), "partition 1 must never be paused");
        // partition 0 is genuinely behind at the moment partition 1 finished
        assertTrue(operations.countForPartition(0) < perPartition || factory.pauseCount(0) >= 1,
                "partition 0 should be throttled/behind while 1 races ahead");

        // --- partition 0 eventually drains, resumes, and loses nothing ---
        await().atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> assertEquals(perPartition, operations.countForPartition(0),
                        "paused partition 0 must eventually persist every record (pause != drop)"));
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertTrue(factory.resumeCount(0) >= 1,
                        "partition 0 must resume once drained"));
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertFalse(factory.isPartitionPaused(0),
                        "nothing left paused at the end"));

        assertEquals(2 * perPartition, operations.totalCount(), "no message lost across both partitions");
        assertEquals(contiguous(perPartition), operations.idsForPartition(0), "partition 0 ids contiguous 1..N");
        assertEquals(contiguous(perPartition), operations.idsForPartition(1), "partition 1 ids contiguous 1..N");
    }

    /** @return the list [1, 2, ..., n] as {@code Long}s. */
    private static List<Long> contiguous(int n) {
        List<Long> ids = new ArrayList<>(n);
        for (long i = 1; i <= n; i++) ids.add(i);
        return ids;
    }
}
