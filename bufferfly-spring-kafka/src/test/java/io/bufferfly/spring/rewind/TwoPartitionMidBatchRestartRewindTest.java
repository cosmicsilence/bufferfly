package io.bufferfly.spring.rewind;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.spring.ShardedActorContainer;
import org.apache.kafka.clients.admin.NewTopic;
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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 Two partitions, ONE consumer, concurrency = 1: a mid-batch restart rewinds
 * <b>partition 0</b> while <b>partition 1</b> keeps going on its own offset.
 *
 * <h2>Why this topology is the point</h2>
 * With {@code concurrency == 1} the single consumer owns <b>both</b> partitions,
 * so one {@code poll()} returns a batch containing records for P0 <em>and</em> P1
 * interleaved (grouped per partition within the batch). The framework must split
 * that batch per partition and route each slice to its own shard. When P0's actor
 * hits an unrecoverable failure mid-batch and restarts (rewinding to its last
 * durably stored offset), only P0's in-flight slice may be discarded and
 * redelivered — <b>P1 must keep committing and advancing its offset, never
 * rewinding and never double-processing</b>.
 *
 * <h2>What is asserted</h2>
 * <ul>
 *   <li>the injected one-shot P0 failure fires and a <em>second</em> P0 actor is
 *       created (restart happened), while P1 keeps its single actor;</li>
 *   <li>P0's {@code applied} log shows at least one offset replayed (the
 *       discarded-and-redelivered in-flight slice), proving the rewind;</li>
 *   <li>P1's {@code applied} log is strictly monotonic with <b>no</b> replay —
 *       P1 never rewound;</li>
 *   <li>the DB ends exactly-once: each partition holds every one of its offsets
 *       exactly once (idempotent insert absorbed P0's replay), no loss, no dup.</li>
 * </ul>
 */
@SpringJUnitConfig(TwoPartitionMidBatchRestartRewindTest.EmptyConfig.class)
@EmbeddedKafka(partitions = 2, topics = {TwoPartitionMidBatchRestartRewindTest.TOPIC})
@Timeout(180)
@DisplayName("🦋 2 partitions / 1 consumer: P0 rewinds mid-batch while P1 keeps its offset")
class TwoPartitionMidBatchRestartRewindTest {

    static final String TOPIC = "bufferfly-rewind-events";

    static class EmptyConfig {
    }

    @Autowired
    private EmbeddedKafkaBroker broker;

    private DataSource dataSource;
    private RewindOperations operations;
    private ShardedActorContainer<String, String> container;
    private KafkaTemplate<String, String> producer;

    private String topic;
    private String group;

    private static final int PER_PARTITION = 60;
    /** Partition-0 offset at/after which the one-shot mid-batch failure fires. */
    private static final long FAIL_P0_AT = 25L;

    @BeforeEach
    void setUp() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        topic = TOPIC + "-" + suffix;
        group = "rewind-group-" + suffix;

        String url = "jdbc:hsqldb:mem:rewind_" + UUID.randomUUID().toString().replace("-", "");
        dataSource = new DriverManagerDataSource(url, "SA", "");
        operations = new RewindOperations(dataSource, topic, FAIL_P0_AT);
        operations.createSchema();

        // Topic with exactly 2 partitions.
        broker.addTopics(new NewTopic(topic, 2, (short) 1));

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        producer = new KafkaTemplate<>(pf);
    }

    @AfterEach
    void tearDown() {
        if (container != null) {
            try {
                container.stop();
            } catch (Exception ignored) {
            }
        }
        if (producer != null) producer.destroy();
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, group, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Keep polls modest so the failure lands genuinely mid-stream, with both
        // partitions' records interleaved across several batches.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Test
    @DisplayName("P0 rewinds mid-batch and replays its in-flight slice; P1 keeps committing with no rewind; DB exactly-once")
    void p0_rewinds_midBatch_while_p1_keeps_going() {
        RewindShardFactory factory = new RewindShardFactory(operations, 10);

        // ONE consumer, concurrency = 1 -> the single consumer owns BOTH partitions,
        // so each poll batch interleaves P0 and P1 records.
        container = new ShardedActorContainer<>(
                group, topic, 1, factory, consumerFactory(), new ActorManager());
        container.start();

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(2, container.shardCount(),
                        "the single consumer must own BOTH partitions (one shard each)"));

        // Interleave records across P0 and P1 so a single poll's batch carries both.
        for (int i = 0; i < PER_PARTITION; i++) {
            producer.send(topic, 0, "k0", "p0-m" + i);
            producer.send(topic, 1, "k1", "p1-m" + i);
        }
        producer.flush();

        // --- the one-shot P0 failure must fire, restarting P0's actor (rewind) ---
        await().atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> assertTrue(operations.p0FailuresTriggered() >= 1,
                        "the injected mid-batch failure on partition 0 must fire"));
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertTrue(factory.actorsCreated(0) >= 2,
                        "partition 0 must restart (a second actor instance is created)"));

        // --- everything persists exactly-once on BOTH partitions ---
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertEquals(PER_PARTITION, operations.countForPartition(0),
                    "partition 0 must persist every record exactly once after its rewind/replay");
            assertEquals(PER_PARTITION, operations.countForPartition(1),
                    "partition 1 must persist every record exactly once");
        });
        // settle: no late duplicates slipped in from the replay
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(2 * PER_PARTITION, operations.totalCount(),
                        "record count must settle at exactly 2 x " + PER_PARTITION + " (no loss, no dup)"));

        // --- P1 was created exactly once: it never restarted/rewound ---
        assertEquals(1, factory.actorsCreated(1),
                "partition 1's actor must NOT restart (only P0 failed)");

        // --- DB content is exactly-once and complete per partition: offsets 0..N-1 ---
        assertEquals(contiguousOffsets(PER_PARTITION), operations.offsetsForPartition(0),
                "partition 0 stored offsets must be exactly 0.." + (PER_PARTITION - 1) + " (exactly-once after replay)");
        assertEquals(contiguousOffsets(PER_PARTITION), operations.offsetsForPartition(1),
                "partition 1 stored offsets must be exactly 0.." + (PER_PARTITION - 1));

        // --- P0 actually REPLAYED: at least one offset appears twice in its
        //     applied log (the discarded in-flight slice that was redelivered) ---
        assertTrue(hasReplay(factory.applied(0)),
                "partition 0 must show a replayed offset (its in-flight slice was discarded and redelivered)");

        // --- P1 NEVER replayed: its applied log is strictly monotonic ---
        assertTrue(isStrictlyMonotonicByOffset(factory.applied(1)),
                "partition 1 must never rewind: its applied offsets are strictly increasing with no replay");
    }

    // -- helpers ------------------------------------------------------------

    private static List<Long> contiguousOffsets(int n) {
        List<Long> offs = new ArrayList<>(n);
        for (long i = 0; i < n; i++) offs.add(i);
        return offs;
    }

    /** @return true if any offset appears more than once in the applied log. */
    private static boolean hasReplay(List<String> appliedLog) {
        List<Long> offsets = offsetsOf(appliedLog);
        java.util.Set<Long> seen = new java.util.HashSet<>();
        for (long o : offsets) {
            if (!seen.add(o)) return true;
        }
        return false;
    }

    /** @return true if applied offsets never decrease and never repeat (no rewind). */
    private static boolean isStrictlyMonotonicByOffset(List<String> appliedLog) {
        List<Long> offsets = offsetsOf(appliedLog);
        for (int i = 1; i < offsets.size(); i++) {
            if (offsets.get(i) <= offsets.get(i - 1)) return false;
        }
        return true;
    }

    /** Extract the offset field from each "partition:offset:value" entry. */
    private static List<Long> offsetsOf(List<String> appliedLog) {
        List<Long> offsets = new ArrayList<>(appliedLog.size());
        for (String e : appliedLog) {
            String[] parts = e.split(":", 3);
            offsets.add(Long.parseLong(parts[1]));
        }
        return offsets;
    }
}
