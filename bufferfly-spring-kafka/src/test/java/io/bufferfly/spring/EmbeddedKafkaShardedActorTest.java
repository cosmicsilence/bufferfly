package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test against an embedded Kafka broker: produces records across
 * partitions and verifies the {@link ShardedActorContainer} starts one actor per
 * partition, routes each record to the correct per-partition actor, and makes
 * progress (offsets acknowledged).
 */
@SpringJUnitConfig(EmbeddedKafkaShardedActorTest.EmptyConfig.class)
@EmbeddedKafka(partitions = 3, topics = {EmbeddedKafkaShardedActorTest.TOPIC})
@Timeout(90)
class EmbeddedKafkaShardedActorTest {

    static final String TOPIC = "bufferfly-embedded-events";

    static class EmptyConfig {
    }

    /** One actor per partition; records what it received and which partition it owns. */
    static final class PartitionActor extends AbstractActor<String> {
        final List<String> received = new CopyOnWriteArrayList<>();
        private final String name;

        PartitionActor(String name) {
            this.name = name;
        }

        @Override
        public void receive(String message) {
            received.add(message);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onError(String message, Exception e) {
        }
    }

    static final class TrackingFactory implements ShardedActorFactory<String> {
        final Map<TopicPartition, PartitionActor> actors = new ConcurrentHashMap<>();
        final Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();

        @Override
        public Actor<String> createActor(TopicPartition tp, KafkaContext ctx) {
            assigned.add(tp);
            PartitionActor actor = new PartitionActor(actorName(tp));
            actors.put(tp, actor);
            return actor;
        }
    }

    private ShardedActorContainer<String, String> container;

    @org.springframework.beans.factory.annotation.Autowired
    private EmbeddedKafkaBroker broker;

    @AfterEach
    void tearDown() {
        if (container != null) container.stop();
    }

    @Test
    void should_runOneActorPerPartition_andDeliverAllRecords() {
        // --- consumer factory wired to the embedded broker ---
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                broker, "bufferfly-embedded-group", false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProps);

        TrackingFactory factory = new TrackingFactory();
        container = new ShardedActorContainer<>(
                "embedded-group", TOPIC, 3, factory, consumerFactory, new ActorManager());
        container.start();

        // wait for partitions to be assigned (3 shards started)
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(3, container.shardCount(),
                        "expected one shard (actor) per partition"));

        // --- produce 30 records across the 3 partitions by key ---
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker.getBrokersAsString());
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(
                producerProps, new StringSerializer(), new StringSerializer());
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);

        final int perPartition = 10;
        for (int p = 0; p < 3; p++) {
            for (int i = 0; i < perPartition; i++) {
                template.send(TOPIC, p, "k" + p, "p" + p + "-m" + i);
            }
        }
        template.flush();

        // --- every record must reach exactly its partition's actor ---
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            int total = factory.actors.values().stream().mapToInt(a -> a.received.size()).sum();
            assertEquals(3 * perPartition, total, "all produced records must be delivered (no loss)");
        });

        // isolation: each actor only saw records for its own partition
        factory.actors.forEach((tp, actor) -> {
            String expectedPrefix = "p" + tp.partition() + "-";
            assertTrue(actor.received.stream().allMatch(v -> v.startsWith(expectedPrefix)),
                    "actor for " + tp + " received a record from another partition: " + actor.received);
            assertEquals(perPartition, actor.received.size(),
                    "actor for " + tp + " should receive exactly " + perPartition + " records");
        });

        assertEquals(Set.of(
                new TopicPartition(TOPIC, 0),
                new TopicPartition(TOPIC, 1),
                new TopicPartition(TOPIC, 2)), factory.assigned);
    }
}
