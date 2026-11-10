package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    static class CountingActor extends AbstractActor<ConsumedRecord<String>> {
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

    private static ConsumerRecord<String, String> rec(TopicPartition tp, long offset, String value) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "k", value);
    }

    private static Map<TopicPartition, OffsetAndMetadata> commit(TopicPartition tp, long next) {
        return Map.of(tp, new OffsetAndMetadata(next));
    }

    private static CountingActor actorFor(CapturingFactory factory, TopicPartition tp) {
        return factory.created.stream()
                .filter(a -> a.name().equals(tp.topic() + "-" + tp.partition()))
                .findFirst().orElseThrow();
    }

    /** Applies the actor's initial preStart seek on the "consumer thread" for {@code tp}. */
    private static void awaitInitialSeekApplied(ShardedActorContainer<String, String> container,
                                                TopicPartition tp, RecordingSeekCallback cb, long offset) {
        await().untilAsserted(() -> {
            container.bindSeekCallback(tp, cb);
            assertTrue(cb.seeks.stream().anyMatch(s -> s.offset() == offset),
                    tp + " initial seek(" + offset + ") must be applied");
        });
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
        // onIdleContainer / poll routing would). The actor's async preStart signal is
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
     * Restart window ({@code awaitingRewind}): while a restarted actor's
     * {@code preStart} has not yet signalled its rewind-seek, the partition's
     * slice is discarded and not committed, so a stale record from the
     * pre-restart poll never reaches the fresh actor. Once the new actor's seek
     * is applied, the next poll is delivered and committed.
     */
    @Test
    void should_discardSlice_duringRestart_untilNewActorRewinds() {
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        List<LatchActor> created = new CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean useLatch = new java.util.concurrent.atomic.AtomicBoolean(false);

        ShardedActorFactory<String> factory = (tp, ctx) -> {
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

        // Restart -> the fresh actor's preStart is blocked on the latch, so the
        // shard is 'awaitingRewind'. A poll arriving now must be discarded.
        useLatch.set(true);
        created.get(0).ctx.restartActor();
        await().untilAsserted(() -> assertTrue(created.size() >= 2, "restart created a fresh actor"));

        assertTrue(container.onBatch(List.of(rec(TP0, 99L, "stale-pre-rewind"))).isEmpty(),
                "a poll arriving before the restarted actor rewound is discarded and not committed");

        // Let the new actor signal seek(5); the consumer thread applies it.
        gate.countDown();
        RecordingSeekCallback cb = new RecordingSeekCallback();
        awaitInitialSeekApplied(container, TP0, cb, 5L);

        // The next poll (the rewound stream) is delivered and committed.
        assertEquals(commit(TP0, 6L), container.onBatch(List.of(rec(TP0, 5L, "after-rewind"))));
        LatchActor restarted = created.get(created.size() - 1);
        await().untilAsserted(() -> assertEquals(List.of("after-rewind"), restarted.received,
                "the restarted actor only ever saw the post-rewind record"));
    }

    /**
     * Two partitions on one consumer. P0's actor rewinds while P1 keeps going:
     * <ol>
     *   <li>the poll carrying P0's stale, higher offsets arrives after the rewind
     *       was signalled: the seek is applied on the consumer thread, P0's slice
     *       is discarded and not committed, and P1 is delivered and committed in the
     *       same poll (no cross-partition commit lag);</li>
     *   <li>the next poll is P0's rewound stream with compaction gaps (the sought
     *       offset 100 is gone): delivered as-is and committed, since the gate
     *       never inspects offsets.</li>
     * </ol>
     */
    @Test
    void should_discardRewindingPartition_andCommitOthers_thenDeliverCompactedReplay() {
        CapturingFactory factory = new CapturingFactory(100L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));
        RecordingSeekCallback cb0 = new RecordingSeekCallback();
        RecordingSeekCallback cb1 = new RecordingSeekCallback();
        awaitInitialSeekApplied(container, TP0, cb0, 100L);
        awaitInitialSeekApplied(container, TP1, cb1, 100L);
        // Actors of one assignment start in parallel, so look them up by partition.
        CountingActor actor0 = actorFor(factory, TP0);
        CountingActor actor1 = actorFor(factory, TP1);

        // P0's actor signals a rewind (as a restarted actor's preStart would).
        actor0.ctx.seek(100L);

        // Poll N: P0's slice is pre-seek (higher, stale offsets); P1 is healthy.
        Map<TopicPartition, OffsetAndMetadata> pollN = container.onBatch(List.of(
                rec(TP0, 150L, "stale-150"), rec(TP0, 151L, "stale-151"),
                rec(TP1, 7L, "p1-7"), rec(TP1, 8L, "p1-8")));

        assertEquals(commit(TP1, 9L), pollN, "only the healthy partition is committed");
        assertEquals(2, cb0.seeks.stream().filter(s -> s.offset() == 100L).count(),
                "P0's rewind seek was applied on the consumer thread");
        await().untilAsserted(() -> assertEquals(List.of("p1-7", "p1-8"), actor1.received));
        assertTrue(actor0.received.isEmpty(), "no stale P0 record reached the actor");

        // Poll N+1: P0's rewound stream; compaction removed 100..102.
        Map<TopicPartition, OffsetAndMetadata> pollN1 = container.onBatch(List.of(
                rec(TP0, 103L, "a"), rec(TP0, 104L, "b"), rec(TP0, 107L, "c"),
                rec(TP1, 9L, "p1-9")));

        assertEquals(Map.of(TP0, new OffsetAndMetadata(108L), TP1, new OffsetAndMetadata(10L)), pollN1);
        await().untilAsserted(() -> assertEquals(List.of("a", "b", "c"), actor0.received,
                "the gapped survivors are delivered as-is"));
        await().untilAsserted(() -> assertEquals(List.of("p1-7", "p1-8", "p1-9"), actor1.received));
    }

    /**
     * A seek signalled before any consumer-thread callback is bound cannot be
     * applied yet, so the partition stays rewinding: its slices are discarded and
     * not committed until the callback arrives and applies the seek.
     */
    @Test
    void should_discardSlice_whileSeekCannotBeAppliedYet() {
        CapturingFactory factory = new CapturingFactory(100L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0));
        CountingActor actor = factory.created.get(0);

        actor.ctx.seek(100L); // no callback bound -> stays pending

        assertTrue(container.onBatch(List.of(rec(TP0, 150L, "stale"))).isEmpty(),
                "pending seek -> slice discarded, not committed");
        assertTrue(actor.received.isEmpty());

        RecordingSeekCallback cb = new RecordingSeekCallback();
        container.bindSeekCallback(TP0, cb); // consumer thread applies the seek
        assertTrue(cb.seeks.stream().anyMatch(s -> s.offset() == 100L), "seek(100) applied");

        assertEquals(commit(TP0, 101L), container.onBatch(List.of(rec(TP0, 100L, "a"))));
        await().untilAsserted(() -> assertEquals(List.of("a"), actor.received));
    }

    @Test
    void should_routeRecordToOwningShardActor() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));

        // Apply each partition's preStart seek(0) on the consumer thread so the
        // shards are no longer rewinding and will deliver the next poll.
        RecordingSeekCallback cb0 = new RecordingSeekCallback();
        RecordingSeekCallback cb1 = new RecordingSeekCallback();
        awaitInitialSeekApplied(container, TP0, cb0, 0L);
        awaitInitialSeekApplied(container, TP1, cb1, 0L);

        assertEquals(commit(TP0, 1L), container.onBatch(List.of(rec(TP0, 0L, "hello"))));

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

    /**
     * Fresh assignment (not a restart): the actor's {@code preStart} loads its
     * state and requests a rewind. Assignment blocks until {@code preStart} has
     * finished, so the rewind is already requested when the first poll for the
     * partition arrives: records from the consumer's old position are discarded
     * and never reach the actor (which would otherwise apply them on top of the
     * state it just loaded and double-count).
     */
    @Test
    void should_discardRecordsOnFreshAssignment_untilActorsSeekIsApplied() {
        CapturingFactory factory = new CapturingFactory(5L); // preStart requests seek(5)
        ShardedActorContainer<String, String> container = newContainer(factory);

        container.onAssigned(List.of(TP0)); // returns only after preStart requested its seek
        CountingActor actor = factory.created.get(0);

        // The very next poll still comes from the consumer's old position.
        assertTrue(container.onBatch(List.of(rec(TP0, 0L, "stale-0"), rec(TP0, 1L, "stale-1"))).isEmpty(),
                "records polled before the actor's seek is applied are discarded, not committed");

        // The consumer thread applies the seek; the next poll starts at offset 5.
        RecordingSeekCallback cb = new RecordingSeekCallback();
        container.bindSeekCallback(TP0, cb);
        assertTrue(cb.seeks.stream().anyMatch(s -> s.offset() == 5L), "seek(5) applied");
        assertEquals(Map.of(TP0, new OffsetAndMetadata(6L)), container.onBatch(List.of(rec(TP0, 5L, "a"))));

        await().untilAsserted(() -> assertEquals(List.of("a"), actor.received,
                "the actor only ever sees records from the position it rewound to"));
    }

    /**
     * A {@code preStart} that fails on assignment (e.g. the database is down
     * while loading state) must not leave the partition without an actor: the
     * container requests a supervised restart, which starts a fresh actor after
     * the backoff.
     */
    @Test
    void should_restartActor_whenPreStartFailsOnAssignment() {
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        List<CountingActor> started = new CopyOnWriteArrayList<>();
        ShardedActorFactory<String> factory = (tp, ctx) -> new CountingActor(tp.topic() + "-" + tp.partition(), ctx, 0L) {
            @Override
            protected void preStart() {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("database unavailable");
                }
                super.preStart();
                started.add(this);
            }
        };
        @SuppressWarnings("unchecked")
        ConsumerFactory<String, String> cf = mock(ConsumerFactory.class);
        ShardedActorContainer<String, String> container =
                new ShardedActorContainer<>("events-group", TOPIC, 3, factory, cf, new ActorManager());
        container.setPausePartitionAction(tp -> { });
        container.setResumePartitionAction(tp -> { });

        container.onAssigned(List.of(TP0)); // first preStart fails; must not throw out of the callback

        await().atMost(java.time.Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals(1, started.size(), "a fresh actor is started after the failure"));
        RecordingSeekCallback cb = new RecordingSeekCallback();
        awaitInitialSeekApplied(container, TP0, cb, 0L);
        assertEquals(commit(TP0, 1L), container.onBatch(List.of(rec(TP0, 0L, "hello"))));
        await().untilAsserted(() -> assertEquals(List.of("hello"), started.get(0).received));
    }

    @Test
    void should_notCommit_whenNoShardOwnsPartition() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        // no assignment

        assertTrue(container.onBatch(List.of(rec(TP0, 7L, "orphan"))).isEmpty(),
                "unowned record is not committed -> redelivered after (re)assignment");
    }

    // ======================================================================
    //  OFFSET COMMITS — driven through the listener objects Spring calls.
    // ======================================================================

    @SuppressWarnings("unchecked")
    private static BatchConsumerAwareMessageListener<String, String> listenerOf(
            ShardedActorContainer<String, String> container) {
        return (BatchConsumerAwareMessageListener<String, String>)
                container.container().getContainerProperties().getMessageListener();
    }

    private static ConsumerAwareRebalanceListener rebalanceListenerOf(ShardedActorContainer<String, String> container) {
        return (ConsumerAwareRebalanceListener) container.container().getContainerProperties()
                .getConsumerRebalanceListener();
    }

    /** Each poll commits its fully delivered partitions asynchronously and never blocks on commitSync. */
    @Test
    @SuppressWarnings("unchecked")
    void should_commitAsync_perPoll_andNeverCommitSync() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));
        awaitInitialSeekApplied(container, TP0, new RecordingSeekCallback(), 0L);
        awaitInitialSeekApplied(container, TP1, new RecordingSeekCallback(), 0L);
        Consumer<String, String> consumer = mock(Consumer.class);

        listenerOf(container).onMessage(List.of(rec(TP0, 0L, "a"), rec(TP1, 4L, "b")), consumer);

        verify(consumer).commitAsync(eq(Map.of(TP0, new OffsetAndMetadata(1L), TP1, new OffsetAndMetadata(5L))),
                any(OffsetCommitCallback.class));
        verify(consumer, never()).commitSync(anyMap());
    }

    /** Revoke and lost only release the shards: nothing is committed synchronously. */
    @Test
    @SuppressWarnings("unchecked")
    void should_notCommitSync_onRevokeOrLost() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));
        awaitInitialSeekApplied(container, TP0, new RecordingSeekCallback(), 0L);
        awaitInitialSeekApplied(container, TP1, new RecordingSeekCallback(), 0L);
        Consumer<String, String> consumer = mock(Consumer.class);
        listenerOf(container).onMessage(List.of(rec(TP0, 0L, "a"), rec(TP1, 4L, "b")), consumer);

        rebalanceListenerOf(container).onPartitionsRevokedBeforeCommit(consumer, List.of(TP0));
        rebalanceListenerOf(container).onPartitionsLost(consumer, List.of(TP1));

        verify(consumer, never()).commitSync(anyMap());
        assertFalse(container.hasShard(TP0), "revoked partition's shard is released");
        assertFalse(container.hasShard(TP1), "lost partition's shard is released");
    }

    /**
     * A quiet partition sharing a consumer with a busy one. The quiet partition's
     * actor requests a rewind (as a restarted actor's {@code preStart} would), but
     * no records arrive for it. The consumer never goes idle — the busy partition
     * keeps it polling — so {@code onIdleContainer} never fires. The rewind must
     * still be applied on the consumer thread during the busy partition's polls,
     * otherwise the quiet partition's unpersisted records are not replayed until
     * new traffic arrives for it.
     */
    @Test
    @SuppressWarnings("unchecked")
    void should_applyQuietPartitionsSeek_duringAPollOfAnotherPartition() {
        CapturingFactory factory = new CapturingFactory(0L);
        ShardedActorContainer<String, String> container = newContainer(factory);
        container.onAssigned(List.of(TP0, TP1));
        RecordingSeekCallback cb0 = new RecordingSeekCallback();
        RecordingSeekCallback cb1 = new RecordingSeekCallback();
        awaitInitialSeekApplied(container, TP0, cb0, 0L);
        awaitInitialSeekApplied(container, TP1, cb1, 0L);
        CountingActor quiet = factory.created.stream()
                .filter(a -> a.name().equals(TP1.topic() + "-" + TP1.partition())).findFirst().orElseThrow();
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.assignment()).thenReturn(Set.of(TP0, TP1));

        quiet.ctx.seek(5L); // TP1 rewinds; no TP1 records are coming

        // The busy partition keeps the consumer polling (so it is never idle).
        listenerOf(container).onMessage(List.of(rec(TP0, 0L, "a")), consumer);
        listenerOf(container).onMessage(List.of(rec(TP0, 1L, "b")), consumer);

        // Accept either mechanism: the stored seek callback, or a direct consumer.seek.
        boolean viaCallback = cb1.seeks.contains(new RecordingSeekCallback.Seek(TOPIC, 1, 5L));
        boolean viaConsumer = mockingDetails(consumer).getInvocations().stream()
                .anyMatch(i -> i.getMethod().getName().equals("seek")
                        && TP1.equals(i.getArgument(0)) && Long.valueOf(5L).equals(i.getArgument(1)));
        assertTrue(viaCallback || viaConsumer,
                "TP1's rewind to offset 5 must be applied while the consumer polls TP0");
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
     * stop and the fresh actor rewinding, records are discarded (not delivered, not committed). This
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
