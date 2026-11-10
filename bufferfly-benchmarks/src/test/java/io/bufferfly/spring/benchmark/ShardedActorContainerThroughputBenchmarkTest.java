package io.bufferfly.spring.benchmark;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;
import io.bufferfly.spring.ShardedActorContainer;
import io.bufferfly.spring.ShardedActorFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 🦋 <b>Benchmark</b>: raw throughput of {@link ShardedActorContainer} — the
 * Spring Kafka integration itself, with no database.
 *
 * <p>The other Kafka benchmarks drive actors from a plain {@code KafkaConsumer},
 * so they do not exercise the container. This one does: a single consumer
 * ({@code concurrency = 1}) owns all {@link #PARTITIONS} partitions, so every
 * poll is split per partition, routed through each partition's rewind gate to
 * its actor, and committed. The actors only count records, so the measured time
 * is the container's poll → route → commit path.
 *
 * <p>Two times are reported per round: from {@code start()} (includes the
 * consumer-group join) and from the first record to the last (steady state).
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(1800)
@DisplayName("🦋 Benchmark: ShardedActorContainer throughput (Kafka -> actors, no DB)")
class ShardedActorContainerThroughputBenchmarkTest {

    private static final String TOPIC = "container-throughput";
    private static final int PARTITIONS = 3;
    private static final int MESSAGES = 300_000;
    private static final int ROUNDS = 3;

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    /** Counts every record it receives; shared counters across a round's actors. */
    static final class CountingActor extends AbstractActor<ConsumedRecord<String>> {
        private final String name;
        private final AtomicLong received;
        private final AtomicLong firstNanos;

        CountingActor(String name, AtomicLong received, AtomicLong firstNanos) {
            this.name = name;
            this.received = received;
            this.firstNanos = firstNanos;
        }

        @Override
        public void receive(ConsumedRecord<String> message) {
            firstNanos.compareAndSet(0L, System.nanoTime());
            received.incrementAndGet();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onError(ConsumedRecord<String> message, Exception e) {
        }
    }

    record Round(long fromStartMs, long steadyMs) {
    }

    @Test
    @DisplayName("every record reaches its actor; container throughput is reported")
    void container_throughput() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
        }
        produce(bootstrap);

        List<Round> rounds = new ArrayList<>();
        for (int round = 0; round < ROUNDS; round++) {
            rounds.add(runRound(bootstrap, round));
        }

        System.out.println();
        System.out.println("==========================================================================================");
        System.out.printf("SHARDED ACTOR CONTAINER THROUGHPUT  —  %d messages, %d partitions, 1 consumer, %d rounds%n",
                MESSAGES, PARTITIONS, ROUNDS);
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf("%-8s %16s %14s %16s %14s%n", "round", "from start ms", "msg/s", "steady ms", "msg/s");
        for (int i = 0; i < rounds.size(); i++) {
            Round r = rounds.get(i);
            System.out.printf("%-8d %16d %14.0f %16d %14.0f%n", i, r.fromStartMs(), MESSAGES / (r.fromStartMs() / 1000.0),
                    r.steadyMs(), MESSAGES / (Math.max(1, r.steadyMs()) / 1000.0));
        }
        long medianStart = rounds.stream().mapToLong(Round::fromStartMs).sorted().toArray()[ROUNDS / 2];
        long medianSteady = rounds.stream().mapToLong(Round::steadyMs).sorted().toArray()[ROUNDS / 2];
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf("MEDIAN   from start: %d ms (%.0f msg/s)   steady: %d ms (%.0f msg/s)%n",
                medianStart, MESSAGES / (medianStart / 1000.0),
                medianSteady, MESSAGES / (Math.max(1, medianSteady) / 1000.0));
        System.out.println("==========================================================================================");
        System.out.println();
    }

    private Round runRound(String bootstrap, int round) throws InterruptedException {
        AtomicLong received = new AtomicLong();
        AtomicLong firstNanos = new AtomicLong();
        ShardedActorFactory<String> factory = new ShardedActorFactory<>() {
            @Override
            public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
                return new CountingActor(actorName(tp), received, firstNanos);
            }
        };

        ActorManager manager = new ActorManager();
        ShardedActorContainer<String, String> container = new ShardedActorContainer<>(
                "throughput-group-" + round, TOPIC, 1, factory, consumerFactory(bootstrap), manager);
        long start = System.nanoTime();
        try {
            container.start();
            while (received.get() < MESSAGES) {
                Thread.sleep(2);
            }
        } finally {
            container.stop();
            manager.shutdown();
        }
        long end = System.nanoTime();
        assertEquals(MESSAGES, received.get(), "every record reaches its actor exactly once");
        return new Round((end - start) / 1_000_000L, (end - firstNanos.get()) / 1_000_000L);
    }

    private static DefaultKafkaConsumerFactory<String, String> consumerFactory(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, BenchmarkConsumers.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private static void produce(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGES; i++) {
                producer.send(new ProducerRecord<>(TOPIC, i % PARTITIONS, "k" + i, "v" + i));
            }
            producer.flush();
        }
    }
}
