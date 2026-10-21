package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Lifecycle, routing and signal-based-seek tests for {@link ShardedActorContainer}
 * driven through its package-private seams — no broker required.
 */
@Timeout(30)
class ShardedActorContainerTest {

    private static final String TOPIC = "events";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    /** Records every value it receives; signals a seek to a stored offset on start. */
    static final class CountingActor extends AbstractActor<ConsumedRecord<String>> {
        final List<String> received = new CopyOnWriteArrayList<>();
        private final String name;
        final KafkaContext ctx;
        private final long storedOffset;

        CountingActor(String name, KafkaContext ctx, long storedOffset) {
            this.name = name;
            this.ctx = ctx;
            this.storedOffset = storedOffset;
        }

        @Override
        protected void preStart() {
            ctx.seek(storedOffset); // "fetch last offset from DB and rewind"
        }

        @Override
        public void receive(ConsumedRecord<String> message) {
            received.add(message.value());
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onError(ConsumedRecord<String> message, Exception e) {
        }
    }

    static final class CapturingFactory implements ShardedActorFactory<String> {
        final List<CountingActor> created = new CopyOnWriteArrayList<>();
        final long storedOffset;

        CapturingFactory(long storedOffset) {
            this.storedOffset = storedOffset;
        }

        @Override
        public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
            CountingActor actor = new CountingActor(actorName(tp), ctx, storedOffset);
            created.add(actor);
            return actor;
        }
    }

    static final class RecordingAck implements Acknowledgment {
        final AtomicInteger acks = new AtomicInteger(0);

        @Override
        public void acknowledge() {
            acks.incrementAndGet();
        }
    }

    /** Captures seek signals relayed by the container on the "consumer thread". */
    static final class RecordingSeekCallback implements ConsumerSeekAware.ConsumerSeekCallback {
        record Seek(String topic, int partition, long offset) {
        }

        final List<Seek> seeks = new CopyOnWriteArrayList<>();

        @Override
        public void seek(String topic, int partition, long offset) {
            seeks.add(new Seek(topic, partition, offset));
        }

        @Override
        public void seekToBeginning(String topic, int partition) {
            seeks.add(new Seek(topic, partition, -1L));
        }

        @Override
        public void seekToEnd(String topic, int partition) {
            seeks.add(new Seek(topic, partition, -2L));
        }

        @Override
        public void seek(String topic, int partition, java.util.function.Function<Long, Long> offsetComputeFunction) {
        }

        @Override
        public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
        }

        @Override
        public void seekToTimestamp(String topic, int partition, long timestamp) {
        }

        @Override
        public void seekToTimestamp(java.util.Collection<TopicPartition> topicPartitions, long timestamp) {
        }
    }

    @SuppressWarnings("unchecked")
    private static ShardedActorContainer<String, String> newContainer(CapturingFactory factory) {
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        return new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager());
    }

    @Test
    void should_startActorOnAssign() {
        CapturingFactory factory = new CapturingFactory(42L);
        ShardedActorContainer<String, String> container = newContainer(factory);

        container.onAssigned(List.of(TP0));

        assertTrue(container.hasShard(TP0));
        assertEquals(1, container.shardCount());
    }

    @Test
    void should_relaySeekSignalToConsumerCallback_whenActorSeeksOnStart() {
        CapturingFactory factory = new CapturingFactory(42L);
        ShardedActorContainer<String, String> container = newContainer(factory);

        // partition assigned -> actor created and (async) preStart signals seek(42)
        container.onAssigned(List.of(TP0));
        RecordingSeekCallback cb = new RecordingSeekCallback();

        // The consumer thread repeatedly binds/flushes (as onPartitionsAssigned /
        // onIdleContainer / onRecord would). The actor's async preStart signal is
        // buffered and relayed to the callback on the next consumer-thread flush.
        await().untilAsserted(() -> {
            container.bindSeekCallback(TP0, cb); // re-flush on "consumer thread"
            assertEquals(1, cb.seeks.size(), "exactly one seek relayed");
            assertEquals(new RecordingSeekCallback.Seek(TOPIC, 0, 42L), cb.seeks.get(0));
        });
    }

    @Test
    void should_preserveSeekCallbackAcrossRestart_soRestartedActorRewinds() {
        CapturingFactory factory = new CapturingFactory(42L);
        ShardedActorContainer<String, String> container = newContainer(factory);

        container.onAssigned(List.of(TP0));
        RecordingSeekCallback cb = new RecordingSeekCallback();

        // First actor's preStart seek(42) is relayed.
        await().untilAsserted(() -> {
            container.bindSeekCallback(TP0, cb);
            assertEquals(1, cb.seeks.size());
        });

        CountingActor first = factory.created.get(0);

        // Trigger an actor restart (as onUnrecoverable would). stopShard must NOT
        // discard the SeekState, so the callback bound above survives and the
        // NEW actor's preStart seek is still relayed.
        first.ctx.restartActor();

        await().untilAsserted(() -> {
            container.bindSeekCallback(TP0, cb); // consumer-thread flush
            // a second actor was created and its rewind seek reached the callback
            assertTrue(factory.created.size() >= 2, "restart created a fresh actor");
            assertEquals(2, cb.seeks.size(), "restarted actor's rewind seek must be relayed");
            assertEquals(new RecordingSeekCallback.Seek(TOPIC, 0, 42L), cb.seeks.get(1));
        });
    }

    @Test
    void should_stopActorOnRevoke() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);

        container.onAssigned(List.of(TP0, TP1));
        assertEquals(2, container.shardCount());

        container.onRevoked(List.of(TP0));
        assertFalse(container.hasShard(TP0));
        assertTrue(container.hasShard(TP1));
        assertEquals(1, container.shardCount());
    }

    @Test
    void should_routeRecordToOwningShardActor() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));

        RecordingAck ack = new RecordingAck();
        boolean handled = container.onRecord(
                new ConsumerRecord<>(TOPIC, 0, 0L, "k", "hello"), ack);

        assertTrue(handled);
        await().untilAsserted(() -> assertEquals(1, ack.acks.get()));

        CountingActor actor0 = factory.created.stream()
                .filter(a -> a.name().equals(TP0.topic() + "-" + TP0.partition())).findFirst().orElseThrow();
        CountingActor actor1 = factory.created.stream()
                .filter(a -> a.name().equals(TP1.topic() + "-" + TP1.partition())).findFirst().orElseThrow();
        await().untilAsserted(() -> assertEquals(List.of("hello"), actor0.received));
        assertTrue(actor1.received.isEmpty());
    }

    @Test
    void should_pauseAndResumeOwnPartition_viaKafkaContext() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        List<TopicPartition> paused = new CopyOnWriteArrayList<>();
        List<TopicPartition> resumed = new CopyOnWriteArrayList<>();
        container.setPausePartitionAction(paused::add);
        container.setResumePartitionAction(resumed::add);

        container.onAssigned(List.of(TP0));
        CountingActor actor0 = factory.created.get(0);

        actor0.ctx.pausePartition();
        actor0.ctx.resumePartition();

        assertEquals(List.of(TP0), paused, "pausePartition() pauses only the actor's own partition");
        assertEquals(List.of(TP0), resumed, "resumePartition() resumes only the actor's own partition");
    }

    @Test
    void should_notAckAndSkip_whenNoShardOwnsPartition() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        // no assignment

        RecordingAck ack = new RecordingAck();
        boolean handled = container.onRecord(
                new ConsumerRecord<>(TOPIC, 0, 7L, "k", "orphan"), ack);

        assertFalse(handled, "record for an unowned partition is not handled");
        assertEquals(0, ack.acks.get(), "unowned record is not acked -> redelivered after (re)assignment");
    }
}
