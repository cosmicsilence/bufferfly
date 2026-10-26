package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.metrics.InMemoryBufferflyMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the {@link ShardedActorContainer} reports the Kafka-lifecycle signals
 * to a wired {@link InMemoryBufferflyMetrics}, labelled by the per-partition
 * actor name, driven through the container's package-private seams (no broker).
 */
@Timeout(30)
class ShardedActorContainerMetricsTest {

    private static final String TOPIC = "events";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final String COMPONENT = TOPIC + "-0";

    static final class CountingActor extends AbstractActor<ConsumedRecord<String>> {
        private final String name;
        final KafkaContext ctx;

        CountingActor(String name, KafkaContext ctx) {
            this.name = name;
            this.ctx = ctx;
        }

        @Override protected void preStart() { ctx.seek(0L); }
        @Override public void receive(ConsumedRecord<String> message) { }
        @Override public String name() { return name; }
        @Override public void onError(ConsumedRecord<String> message, Exception e) { }
    }

    static final class CapturingFactory implements ShardedActorFactory<String> {
        final List<CountingActor> created = new CopyOnWriteArrayList<>();
        @Override
        public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
            CountingActor a = new CountingActor(actorName(tp), ctx);
            created.add(a);
            return a;
        }
    }

    static final class RecordingAck implements Acknowledgment {
        final AtomicInteger acks = new AtomicInteger();
        @Override public void acknowledge() { acks.incrementAndGet(); }
    }

    @SuppressWarnings("unchecked")
    private static ShardedActorContainer<String, String> newContainer(
            CapturingFactory factory, InMemoryBufferflyMetrics metrics) {
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        return new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager(), metrics);
    }

    @Test
    void should_countPartitionAssignedAndRevoked() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        ShardedActorContainer<String, String> container = newContainer(new CapturingFactory(), metrics);

        container.onAssigned(List.of(TP0));
        assertEquals(1, metrics.partitionAssignedFor(COMPONENT), "assign recorded under the actor name");

        container.onRevoked(List.of(TP0));
        assertEquals(1, metrics.partitionRevokedFor(COMPONENT), "revoke recorded under the actor name");
    }

    @Test
    void should_countPartitionPauseAndResume_viaKafkaContext() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        CapturingFactory factory = new CapturingFactory();
        ShardedActorContainer<String, String> container = newContainer(factory, metrics);
        container.setPausePartitionAction(tp -> { });
        container.setResumePartitionAction(tp -> { });

        container.onAssigned(List.of(TP0));
        CountingActor actor = factory.created.get(0);

        actor.ctx.pausePartition();
        actor.ctx.resumePartition();

        assertEquals(1, metrics.partitionPausedFor(COMPONENT), "pause recorded");
        assertEquals(1, metrics.partitionResumedFor(COMPONENT), "resume recorded");
    }

    @Test
    void should_countRecordRejected_whenNoShardOwnsPartition() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        ShardedActorContainer<String, String> container = newContainer(new CapturingFactory(), metrics);
        // no assignment -> the record is rejected (un-acked, will redeliver)

        RecordingAck ack = new RecordingAck();
        container.onRecord(new ConsumerRecord<>(TOPIC, 0, 7L, "k", "orphan"), ack);

        assertEquals(1, metrics.recordRejectedFor(COMPONENT), "the un-owned record is counted as rejected");
        assertEquals(0, ack.acks.get(), "and not acked");
    }

    @Test
    void should_countActorRestarted_andPauseResume_onRestart() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        CapturingFactory factory = new CapturingFactory();
        ShardedActorContainer<String, String> container = newContainer(factory, metrics);
        container.setPausePartitionAction(tp -> { });
        container.setResumePartitionAction(tp -> { });

        container.onAssigned(List.of(TP0));
        await().untilAsserted(() -> assertEquals(1, factory.created.size()));

        // Trigger a supervised restart (as an onUnrecoverable hook would).
        factory.created.get(0).ctx.restartActor();

        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(1, metrics.actorRestartedFor(COMPONENT),
                        "the restart (fresh actor started) is recorded once"));
        // The restart also paused and then resumed the partition.
        assertTrue(metrics.partitionPausedFor(COMPONENT) >= 1, "restart paused the partition");
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertTrue(metrics.partitionResumedFor(COMPONENT) >= 1,
                        "restart resumed the partition"));
    }
}
