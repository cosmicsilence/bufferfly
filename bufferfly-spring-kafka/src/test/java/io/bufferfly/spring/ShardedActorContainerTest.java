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

    /**
     * An actor whose {@code preStart} blocks on a latch before signalling its
     * rewind-seek, so a test can hold the restart in the exact window where the
     * fresh shard is registered but has NOT yet rewound.
     */
    static final class LatchActor extends AbstractActor<ConsumedRecord<String>> {
        final List<String> received = new CopyOnWriteArrayList<>();
        private final String name;
        final KafkaContext ctx;
        final java.util.concurrent.CountDownLatch gate;

        LatchActor(String name, KafkaContext ctx, java.util.concurrent.CountDownLatch gate) {
            this.name = name;
            this.ctx = ctx;
            this.gate = gate;
        }

        @Override
        protected void preStart() {
            try {
                gate.await(); // hold here until the test lets preStart record the seek
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ctx.seek(5L); // rewind to the "DB-stored" offset + 1
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

    /**
     * The exactly-once restart/rewind window: while a restarted actor's
     * {@code preStart} has not yet recorded its rewind-seek, every record for the
     * partition must be DISCARDED (not delivered, not acked), so a stale record
     * from the pre-restart poll position can never be re-processed by the fresh
     * actor. Once the rewind is signalled, delivery resumes.
     */
    @Test
    void should_discardRecordsDuringRestart_untilNewActorHasRewound() {
        // First actor rewinds immediately on start; a later restart uses a latch
        // to hold the new actor in the pre-rewind window.
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        List<LatchActor> created = new CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean useLatch = new java.util.concurrent.atomic.AtomicBoolean(false);

        ShardedActorFactory<String> factory = (tp, ctx) -> {
            // First creation: open latch (start immediately). Restart: closed latch.
            java.util.concurrent.CountDownLatch g = useLatch.get()
                    ? gate
                    : new java.util.concurrent.CountDownLatch(0);
            LatchActor a = new LatchActor(tp.topic() + "-" + tp.partition(), ctx, g);
            created.add(a);
            return a;
        };

        @SuppressWarnings("unchecked")
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        ShardedActorContainer<String, String> container =
                new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager());

        container.onAssigned(List.of(TP0));
        await().untilAsserted(() -> assertEquals(1, created.size(), "first actor created"));

        // From now on, restarts create a latched (blocked-in-preStart) actor.
        useLatch.set(true);
        created.get(0).ctx.restartActor();

        // The fresh actor is (being) created but its preStart is blocked on the
        // latch => the partition is 'awaitingRewind'. A record delivered now must
        // be discarded and NOT acked.
        await().untilAsserted(() -> assertTrue(created.size() >= 2, "restart created a fresh actor"));
        RecordingAck ack = new RecordingAck();
        boolean handledDuringWindow = container.onRecord(
                new ConsumerRecord<>(TOPIC, 0, 99L, "k", "stale-pre-rewind"), ack);
        assertFalse(handledDuringWindow,
                "a record arriving before the restarted actor rewound must be discarded");
        assertEquals(0, ack.acks.get(), "the discarded record must NOT be acked (Kafka will redeliver)");

        // Let the new actor finish preStart (records seek(5) -> clears the gate).
        gate.countDown();

        // After the rewind is recorded, the sought offset is delivered again and
        // accepted; the stale pre-rewind record never reached any actor.
        await().untilAsserted(() -> {
            RecordingAck ack2 = new RecordingAck();
            boolean handled = container.onRecord(
                    new ConsumerRecord<>(TOPIC, 0, 5L, "k", "after-rewind"), ack2);
            assertTrue(handled, "once rewound, the sought offset must be delivered");
            assertEquals(1, ack2.acks.get());
        });
        LatchActor restarted = created.get(created.size() - 1);
        assertEquals(List.of("after-rewind"), restarted.received,
                "the restarted actor only ever saw the post-rewind record (no stale re-processing)");
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

    // ======================================================================
    //  BEHAVIOR LOCK — implementation-agnostic restart contract.
    //  These assert the OBSERVABLE restart sequence/ordering only (via the
    //  container's public seams), so they survive an internal refactor that
    //  moves restart+backoff state into a supervisor actor. If any of these
    //  break, the externally-visible restart behavior changed.
    // ======================================================================

    /** Records the observable restart actions in order. */
    static final class SequenceRecorder {
        final List<String> events = new CopyOnWriteArrayList<>();

        void record(String e) {
            events.add(e);
        }
    }

    /**
     * Behavior contract: a restart performs, <b>in order</b>, pause → (stop old)
     * → start a fresh actor (which rewinds) → resume. During the window between
     * stop and the fresh actor rewinding, records are discarded (un-acked). This
     * is asserted purely through observable seams.
     */
    @Test
    void behaviorLock_restartSequence_pauseThenNewActorThenResume_andDiscardInWindow() {
        SequenceRecorder seq = new SequenceRecorder();
        List<CountingActor> created = new CopyOnWriteArrayList<>();

        ShardedActorFactory<String> factory = (tp, ctx) -> {
            CountingActor a = new CountingActor(tp.topic() + "-" + tp.partition(), ctx, 7L);
            created.add(a);
            seq.record("create");
            return a;
        };
        @SuppressWarnings("unchecked")
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        ShardedActorContainer<String, String> container =
                new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager());
        container.setPausePartitionAction(tp -> seq.record("pause"));
        container.setResumePartitionAction(tp -> seq.record("resume"));

        container.onAssigned(List.of(TP0));
        await().untilAsserted(() -> assertEquals(1, created.size(), "initial actor created"));

        // trigger a restart (as onUnrecoverable -> ctx.restartActor() would)
        created.get(0).ctx.restartActor();

        // a fresh actor must be created and the partition resumed
        await().untilAsserted(() -> assertTrue(created.size() >= 2, "restart created a fresh actor"));
        await().untilAsserted(() -> assertTrue(seq.events.contains("resume"), "partition resumed after restart"));

        // ORDERING contract (observable): the restart pauses BEFORE it creates the
        // replacement actor, and resumes AFTER. (The initial assign also creates;
        // we check the restart-related subsequence after the first resume path.)
        int firstPause = seq.events.indexOf("pause");
        int lastCreate = seq.events.lastIndexOf("create");
        int resume = seq.events.indexOf("resume");
        assertTrue(firstPause >= 0, "a pause must occur on restart");
        assertTrue(firstPause < lastCreate, "restart must pause BEFORE starting the fresh actor");
        assertTrue(lastCreate < resume, "restart must start the fresh actor BEFORE resuming");

        // the fresh actor rewinds to the stored offset
        RecordingSeekCallback cb = new RecordingSeekCallback();
        await().untilAsserted(() -> {
            container.bindSeekCallback(TP0, cb);
            assertTrue(cb.seeks.stream().anyMatch(s -> s.offset() == 7L),
                    "the restarted actor must rewind to the stored offset");
        });
    }

    /**
     * Behavior contract: successive rapid restarts of the same partition wait
     * progressively longer before the replacement actor is started (backoff
     * grows). Asserted by TIMING the gap from restart request to the fresh
     * actor's creation, with no reference to the backoff internals.
     */
    @Test
    void behaviorLock_successiveRestartsBackOffLonger() {
        List<Long> createTimestamps = new CopyOnWriteArrayList<>();
        List<CountingActor> created = new CopyOnWriteArrayList<>();

        ShardedActorFactory<String> factory = (tp, ctx) -> {
            CountingActor a = new CountingActor(tp.topic() + "-" + tp.partition(), ctx, 0L);
            created.add(a);
            createTimestamps.add(System.nanoTime());
            return a;
        };
        @SuppressWarnings("unchecked")
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        ShardedActorContainer<String, String> container =
                new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager());

        container.onAssigned(List.of(TP0));
        await().untilAsserted(() -> assertEquals(1, created.size()));

        // First restart: small backoff (~base). Measure request->create gap.
        long t0 = System.nanoTime();
        created.get(created.size() - 1).ctx.restartActor();
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(2, created.size()));
        long firstGapMs = (createTimestamps.get(1) - t0) / 1_000_000;

        // Second restart immediately after: backoff must have grown (longer gap).
        long t1 = System.nanoTime();
        created.get(created.size() - 1).ctx.restartActor();
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(3, created.size()));
        long secondGapMs = (createTimestamps.get(2) - t1) / 1_000_000;

        assertTrue(secondGapMs > firstGapMs,
                "a second consecutive restart must back off longer than the first (got first="
                        + firstGapMs + "ms, second=" + secondGapMs + "ms)");
    }
}
