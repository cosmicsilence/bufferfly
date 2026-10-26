package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.core.persistence.TransactionalOperations;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Full-context integration test for {@link BufferflyHealthIndicator} over
 * embedded Kafka. Proves:
 * <ol>
 *   <li>the indicator is <b>auto-registered</b> by
 *       {@link BufferflySpringAutoConfiguration} (the health API is on the test
 *       classpath, so {@code @ConditionalOnClass(HealthIndicator)} matches);</li>
 *   <li>it reports <b>UP</b> once the sharded listener container is running;</li>
 *   <li>it reports <b>DOWN</b> after the container is stopped — the one liveness
 *       fact a pod restart would fix.</li>
 * </ol>
 */
@SpringJUnitConfig
@EmbeddedKafka(partitions = 2, topics = {"health-events"})
@Timeout(120)
@DisplayName("🩺 BufferflyHealthIndicator: UP while the sharded listener runs, DOWN when it stops")
class BufferflyHealthIndicatorIntegrationTest {

    static final String TOPIC = "health-events";

    @Configuration
    @Import(BufferflySpringAutoConfiguration.class)
    static class TestConfig {

        @Bean
        HealthFactory healthFactory() {
            return new HealthFactory();
        }

        @Bean
        @SuppressWarnings({"unchecked", "rawtypes"})
        ConsumerFactory<Object, Object> consumerFactory(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = new HashMap<>();
            props.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            props.put(GROUP_ID_CONFIG, "health");
            return new DefaultKafkaConsumerFactory(props,
                    new org.apache.kafka.common.serialization.StringDeserializer(),
                    new org.apache.kafka.common.serialization.StringDeserializer());
        }
    }

    /** A trivial @ShardedActor factory; the actor does nothing but exist. */
    @ShardedActor(name = "health", topic = TOPIC, concurrency = 1, highWatermark = 100, lowWatermark = 10)
    static final class HealthFactory implements ShardedActorFactory<String> {
        @Override
        public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
            PersistenceConfig<String> config = PersistenceConfig.builder(new NoopOps()).build();
            return new NoopActor(actorName(tp), config);
        }
    }

    static final class NoopActor extends AbstractPersistentActorHolder {
        NoopActor(String name, PersistenceConfig<String> config) { super(name, config); }
    }

    /** Minimal persistent actor base to avoid repeating boilerplate. */
    abstract static class AbstractPersistentActorHolder
            extends io.bufferfly.core.persistence.AbstractPersistentActor<ConsumedRecord<String>, String> {
        private final String name;
        AbstractPersistentActorHolder(String name, PersistenceConfig<String> config) { super(config); this.name = name; }
        @Override public PersistenceRunner.PersistenceEvent<String> apply(ConsumedRecord<String> m) {
            return PersistenceRunner.newEvent(m.value());
        }
        @Override public PersistenceRunner.SideEffectProcessor process() { return se -> { }; }
        @Override public String name() { return name; }
        @Override public void onError(ConsumedRecord<String> m, Exception e) { }
    }

    static final class NoopOps implements TransactionalOperations<String> {
        @Override public long persistOnTransaction(List<String> batch, long t) { return t; }
        @Override public long onPoisonPill(String e, long t, Exception c) { return t; }
    }

    @Autowired
    private BufferflyHealthIndicator indicator;
    @Autowired
    private ShardedActorBeanPostProcessor postProcessor;
    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    @org.springframework.test.annotation.DirtiesContext(
            methodMode = org.springframework.test.annotation.DirtiesContext.MethodMode.AFTER_METHOD)
    void should_beUpWhileRunning_thenDownAfterStop() {
        // 1) The indicator was auto-registered (bean present) and reports UP once
        //    the container is running and has been assigned its partition.
        await().atMost(java.time.Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(Status.UP, indicator.health().getStatus(),
                        "indicator must be UP while the sharded listener container is running"));

        // 2) Stop the container(s) — the one condition a pod restart would fix.
        postProcessor.containers().forEach(ShardedActorContainer::stop);

        await().atMost(java.time.Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(Status.DOWN, indicator.health().getStatus(),
                        "once the listener is stopped the indicator must be DOWN"));
    }

    /**
     * A rebalance must NOT flip the indicator to DOWN. A competing consumer in
     * the <b>same group</b> that does nothing but {@code poll()} joins the group
     * and steals one of the two partitions from BufferFly's container. That is a
     * genuine rebalance — BufferFly's container revokes a partition (stops that
     * partition's actor and drops its shard) — but the <b>container keeps
     * running</b> with the partition it still owns. The health check, being a
     * stateless {@code isRunning()} aggregate, stays UP throughout; it never
     * inspects partition-level state, so a revoke can't make it stale.
     */
    @Test
    void should_stayUp_when_aCompetingConsumerStealsAPartition() {
        // Wait until BufferFly owns BOTH partitions and is UP.
        await().atMost(java.time.Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    assertEquals(Status.UP, indicator.health().getStatus());
                    assertEquals(2, postProcessor.containers().get(0).shardCount(),
                            "BufferFly should start out owning both partitions");
                });

        // Start a plain competing consumer in the SAME group that just polls and
        // does nothing. Joining the group forces a rebalance that reassigns one
        // partition away from BufferFly's container.
        Map<String, Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(GROUP_ID_CONFIG, "health"); // same group id as the @ShardedActor container
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        Thread poller;
        try (KafkaConsumer<String, String> competitor =
                     new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            competitor.subscribe(List.of(TOPIC));

            java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
            poller = new Thread(() -> {
                try {
                    while (running.get()) {
                        ConsumerRecords<String, String> recs = competitor.poll(Duration.ofMillis(200));
                        // do nothing with the records — we only want to hold a partition
                    }
                } catch (org.apache.kafka.common.errors.WakeupException ignored) {
                    // expected on close
                }
            }, "competing-consumer");
            poller.setDaemon(true);
            poller.start();

            try {
                // The rebalance happens: BufferFly's container gives up one partition
                // (shardCount drops from 2 to 1) — proving a real revoke occurred.
                await().atMost(java.time.Duration.ofSeconds(60))
                        .untilAsserted(() -> assertEquals(1, postProcessor.containers().get(0).shardCount(),
                                "the competing consumer must steal exactly one partition"));

                // THE POINT: throughout the rebalance and after it, the indicator
                // stays UP — the container is still running, just with one partition.
                assertEquals(Status.UP, indicator.health().getStatus(),
                        "a rebalance that revokes a partition must NOT make the health check DOWN");
                // Hold briefly to prove it settles UP (no flapping to DOWN).
                await().during(java.time.Duration.ofSeconds(3)).atMost(java.time.Duration.ofSeconds(10))
                        .untilAsserted(() -> assertEquals(Status.UP, indicator.health().getStatus(),
                                "the indicator must remain UP while owning the surviving partition"));
            } finally {
                running.set(false);
                competitor.wakeup();
            }
        }
        try {
            poller.join(Duration.ofSeconds(10).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
