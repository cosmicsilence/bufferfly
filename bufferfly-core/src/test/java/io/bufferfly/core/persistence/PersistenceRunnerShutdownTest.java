package io.bufferfly.core.persistence;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the <b>shutdown semantics</b> of the persistence runner, so the
 * discard-not-flush contract documented on {@link PersistenceRunner#stop()}
 * cannot silently change.
 *
 * <p>The contract: on {@code stop()} the runner stops accepting events and the
 * dispatcher <em>cancels its in-flight drain and clears the buffer</em> — events
 * enqueued but not yet in a committed batch are discarded from memory. This is
 * safe because durability is anchored to the committed offset (a restart replays
 * them from the source); there is deliberately no synchronous drain-on-stop.
 */
@Timeout(30)
class PersistenceRunnerShutdownTest {

    private final ActorManager manager = new ActorManager();
    private ActorReference<String> ref;

    @AfterEach
    void tearDown() {
        if (ref != null) ref.stop();
        manager.shutdown();
    }

    /**
     * A store whose first {@code persistOnTransaction} blocks on a latch, so the
     * test can deterministically hold the in-flight batch while more events pile
     * up in the dispatcher buffer, then stop the runner.
     */
    static final class BlockingOps implements TransactionalOperations<String> {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch firstBatchEntered = new CountDownLatch(1);
        final List<String> persisted = new CopyOnWriteArrayList<>();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public long persistOnTransaction(List<String> batch, long currentToken) {
            if (calls.getAndIncrement() == 0) {
                firstBatchEntered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS); // hold the first batch in-flight
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            persisted.addAll(batch);
            return currentToken;
        }

        @Override
        public long onPoisonPill(String event, long currentToken, Exception cause) {
            return currentToken;
        }
    }

    /** Minimal persistent actor: enqueues the string value as the persistence event. */
    static final class StringActor extends AbstractPersistentActor<String, String> {
        private final String name;
        StringActor(String name, PersistenceConfig<String> config) { super(config); this.name = name; }
        @Override public PersistenceRunner.PersistenceEvent<String> apply(String m) { return PersistenceRunner.newEvent(m); }
        @Override public PersistenceRunner.SideEffectProcessor process() { return se -> { }; }
        @Override public String name() { return name; }
        @Override public void onError(String m, Exception e) { }
    }

    @Test
    void should_discardBufferedEvents_when_stoppedMidFlight_notFlushThem() throws InterruptedException {
        BlockingOps ops = new BlockingOps();
        // batchSize 1 so the very first enqueued event forms a batch and enters
        // persistOnTransaction (where it blocks), leaving later events buffered.
        // Use an explicit dispatcher instance so we can observe its buffer size().
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(1, 5L);
        PersistenceConfig<String> config = PersistenceConfig.builder(ops)
                .batchSize(1).timeoutMs(5L).maxRetries(0).dispatcher(dispatcher).build();
        StringActor actor = new StringActor("shutdown-actor-" + UUID.randomUUID(), config);
        ref = manager.start(actor);

        // 1) Enqueue the first event; it becomes a batch and blocks in-flight.
        ref.tell("first");
        assertTrue(ops.firstBatchEntered.await(5, TimeUnit.SECONDS),
                "the first batch must enter persistOnTransaction and block");

        // 2) Pile up more events; with the single-writer dispatcher busy on the
        //    blocked first batch, these sit in the buffer, unpersisted.
        for (int i = 0; i < 20; i++) ref.tell("buffered-" + i);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> dispatcher.size() > 0);
        int buffered = dispatcher.size();
        assertTrue(buffered > 0, "there must be events buffered behind the blocked batch");

        // 3) Stop the runner while the batch is still in flight and events buffered.
        ref.stop();

        // The dispatcher buffer is cleared immediately (discard, not drain).
        assertEquals(0, dispatcher.size(),
                "stop() must clear the dispatcher buffer (discard buffered events)");

        // 4) Release the blocked first batch and prove nothing new is persisted:
        //    the runner is STOPPED, so persistBatch is a no-op and the buffered
        //    events were discarded — only 'first' (already in-flight) persists.
        ops.release.countDown();

        // Give any (incorrectly) rescheduled drain a chance to misbehave, then assert.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8))
                .untilAsserted(() -> {
                    assertFalse(ops.persisted.stream().anyMatch(s -> s.startsWith("buffered-")),
                            "buffered events must be discarded on stop, never flushed");
                    assertEquals(List.of("first"), ops.persisted,
                            "only the already-in-flight batch persists; the rest are dropped for replay");
                });
    }

    @Test
    void should_rejectNewEnqueues_afterStop() {
        BlockingOps ops = new BlockingOps();
        ops.release.countDown(); // don't block; this test is only about post-stop enqueue
        PersistenceConfig<String> config = PersistenceConfig.builder(ops)
                .batchSize(10).timeoutMs(5L).maxRetries(0).build();
        StringActor actor = new StringActor("reject-actor-" + UUID.randomUUID(), config);
        ActorReference<String> local = manager.start(actor);
        BatchingPersistenceRunner<String> runner = (BatchingPersistenceRunner<String>) actor.runner();

        local.stop();

        assertThrows(IllegalStateException.class,
                () -> runner.enqueue(PersistenceRunner.newEvent("after-stop")),
                "a stopped runner must refuse new events");
    }
}
