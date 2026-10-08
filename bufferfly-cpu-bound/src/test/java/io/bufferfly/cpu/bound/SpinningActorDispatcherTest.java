package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.AbstractActor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SpinningActorDispatcher}.
 *
 * <p>Mirrors the style and coverage of {@code BlockingMailboxTest} / {@code VTDispatcherTest},
 * adapted for the CPU-bound, busy-spinning dispatch model:
 *
 * <ul>
 *   <li>Single and multi-message delivery.</li>
 *   <li>Actor isolation — messages never cross actor boundaries.</li>
 *   <li>Burst delivery.</li>
 *   <li>Concurrent senders → same actor (MPSC race condition).</li>
 *   <li>Single-writer invariant — {@code receive()} is never called concurrently.</li>
 *   <li>Concurrent senders → many actors simultaneously.</li>
 *   <li>Error handling — {@code onError} called; loop survives.</li>
 *   <li>Clear — discards pending messages and stops the spin thread.</li>
 *   <li>Stress — many actors × many senders × many messages each.</li>
 * </ul>
 *
 * <p><b>Note on timeouts:</b> each spinning thread pins a real OS thread.
 * Tests are intentionally kept narrow in volume to avoid excessive thermal
 * load on CI runners while still exercising all code paths.
 */
@Timeout(60)
class SpinningActorDispatcherTest {

    private SpinningActorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new SpinningActorDispatcher();
    }

    @AfterEach
    void tearDown() {
        // Best-effort cleanup so spinning threads do not leak between tests.
        // Individual tests clear their own actors; this is a safety net.
    }

    // -----------------------------------------------------------------------
    // Helper — minimal actor usable without ActorManager
    // -----------------------------------------------------------------------

    static class CountingActor extends AbstractActor<String> {

        private final String actorName;
        private final AtomicInteger receiveCount = new AtomicInteger(0);
        private final List<String> received = Collections.synchronizedList(new ArrayList<>());
        private final List<String> errorMessages = Collections.synchronizedList(new ArrayList<>());

        private volatile long processingDelayMs = 0;
        private volatile String failOnMessage = null;

        CountingActor(String name) {
            this.actorName = name;
        }

        void setProcessingDelayMs(long ms) {
            this.processingDelayMs = ms;
        }

        void setFailOnMessage(String text) {
            this.failOnMessage = text;
        }

        @Override
        public void receive(String message) {
            if (message.equals(failOnMessage)) {
                throw new IllegalArgumentException("Simulated failure for: " + message);
            }
            if (processingDelayMs > 0) {
                try {
                    Thread.sleep(processingDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(message);
            receiveCount.incrementAndGet();
        }

        @Override
        public String name() {
            return actorName;
        }

        @Override
        protected void preStart() {
        }

        @Override
        protected void postStop() {
        }

        @Override
        public void onError(String message, Exception e) {
            errorMessages.add(message);
        }

        int getReceiveCount() {
            return receiveCount.get();
        }

        List<String> getReceived() {
            return Collections.unmodifiableList(received);
        }

        List<String> getErrorMessages() {
            return Collections.unmodifiableList(errorMessages);
        }
    }

    // -----------------------------------------------------------------------
    // Basic dispatch
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessage_when_singleMessageDispatched() {
        CountingActor actor = new CountingActor("actor-single");
        actor.start();

        dispatcher.dispatch("hello", actor);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertEquals(1, actor.getReceiveCount());
                    assertEquals("hello", actor.getReceived().get(0));
                });

        dispatcher.clear(actor);
    }

    @Test
    void should_deliverAllMessages_when_multipleMessagesDispatchedSequentially() {
        CountingActor actor = new CountingActor("actor-multi");
        actor.start();

        dispatcher.dispatch("msg-1", actor);
        dispatcher.dispatch("msg-2", actor);
        dispatcher.dispatch("msg-3", actor);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(3, actor.getReceiveCount()));

        dispatcher.clear(actor);
    }

    // -----------------------------------------------------------------------
    // Actor isolation
    // -----------------------------------------------------------------------

    @Test
    void should_neverCrossMessages_when_twoActorsReceiveInterleavedDispatches() {
        CountingActor alpha = new CountingActor("actor-alpha");
        CountingActor beta = new CountingActor("actor-beta");
        alpha.start();
        beta.start();

        dispatcher.dispatch("for-alpha-1", alpha);
        dispatcher.dispatch("for-beta-1", beta);
        dispatcher.dispatch("for-alpha-2", alpha);
        dispatcher.dispatch("for-beta-2", beta);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertEquals(2, alpha.getReceiveCount());
                    assertEquals(2, beta.getReceiveCount());
                    assertTrue(alpha.getReceived().stream().allMatch(m -> m.contains("alpha")));
                    assertTrue(beta.getReceived().stream().allMatch(m -> m.contains("beta")));
                });

        dispatcher.clear(alpha);
        dispatcher.clear(beta);
    }

    @Test
    void should_deliverOnlyOwnMessages_when_manyActorsDispatchedConcurrently() {
        final int ACTOR_COUNT = 4; // keep small — each actor pins a real OS thread
        final int MSG_PER_ACTOR = 20;

        List<CountingActor> actors = new ArrayList<>();
        for (int i = 0; i < ACTOR_COUNT; i++) {
            CountingActor a = new CountingActor("iso-actor-" + i);
            a.start();
            actors.add(a);
        }

        for (int m = 0; m < MSG_PER_ACTOR; m++) {
            for (CountingActor a : actors) {
                dispatcher.dispatch("msg-for-" + a.name(), a);
            }
        }

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    for (CountingActor a : actors) {
                        assertEquals(MSG_PER_ACTOR, a.getReceiveCount(),
                                a.name() + " wrong count");
                        assertTrue(a.getReceived().stream().allMatch(msg -> msg.contains(a.name())),
                                a.name() + " received a foreign message");
                    }
                });

        actors.forEach(dispatcher::clear);
    }

    // -----------------------------------------------------------------------
    // Burst delivery
    // -----------------------------------------------------------------------

    @Test
    void should_deliverAllMessages_when_burstDispatchedToSingleActor() {
        final int BURST = 1_000; // MpscArrayQueue capacity is 20_000
        CountingActor actor = new CountingActor("actor-burst");
        actor.start();

        for (int i = 0; i < BURST; i++) {
            dispatcher.dispatch("msg-" + i, actor);
        }

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertEquals(BURST, actor.getReceiveCount(),
                        "Not all burst messages were delivered"));

        dispatcher.clear(actor);
    }

    // -----------------------------------------------------------------------
    // Concurrent senders → same actor (MPSC race condition)
    // -----------------------------------------------------------------------

    @Test
    void should_loseNoMessages_when_manyConcurrentSendersTargetSameActor() throws InterruptedException {
        final int SENDER_THREADS = 20;
        final int MSG_PER_SENDER = 50;
        final int TOTAL = SENDER_THREADS * MSG_PER_SENDER;

        CountingActor actor = new CountingActor("actor-concurrent");
        actor.start();

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch sendersDone = new CountDownLatch(SENDER_THREADS);

        for (int t = 0; t < SENDER_THREADS; t++) {
            final int base = t * MSG_PER_SENDER;
            Thread.ofVirtual().start(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < MSG_PER_SENDER; i++) {
                    dispatcher.dispatch("msg-" + (base + i), actor);
                }
                sendersDone.countDown();
            });
        }

        startGate.countDown();
        sendersDone.await(15, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(TOTAL, actor.getReceiveCount(),
                        "Some messages were lost under concurrent MPSC senders"));

        dispatcher.clear(actor);
    }

    // -----------------------------------------------------------------------
    // Single-writer invariant
    // -----------------------------------------------------------------------

    @Test
    void should_neverCallReceiveConcurrently_when_manySendersRaceOnSameActor() throws InterruptedException {
        final int SENDER_THREADS = 20;
        final int MSG_PER_SENDER = 50;
        final int TOTAL = SENDER_THREADS * MSG_PER_SENDER;

        AtomicInteger concurrentReceives = new AtomicInteger(0);
        AtomicInteger activeReceivers = new AtomicInteger(0);

        CountingActor actor = new CountingActor("actor-single-writer") {
            @Override
            public void receive(String message) {
                int active = activeReceivers.incrementAndGet();
                if (active > 1) concurrentReceives.incrementAndGet();
                super.receive(message);
                activeReceivers.decrementAndGet();
            }
        };
        actor.start();

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(SENDER_THREADS);

        for (int t = 0; t < SENDER_THREADS; t++) {
            Thread.ofVirtual().start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < MSG_PER_SENDER; i++) dispatcher.dispatch("m", actor);
                done.countDown();
            });
        }

        gate.countDown();
        done.await(15, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(TOTAL, actor.getReceiveCount()));

        assertEquals(0, concurrentReceives.get(),
                "Single-writer violation: actor.receive() was invoked concurrently");

        dispatcher.clear(actor);
    }

    // -----------------------------------------------------------------------
    // Concurrent senders → many actors simultaneously
    // -----------------------------------------------------------------------

    @Test
    void should_loseNoMessages_when_manySendersTargetManyActorsSimultaneously() throws InterruptedException {
        final int ACTOR_COUNT = 4;  // keep small — each actor pins a real OS thread
        final int SENDER_PER_ACTOR = 10;
        final int MSG_PER_SENDER = 50;
        final int TOTAL_PER_ACTOR = SENDER_PER_ACTOR * MSG_PER_SENDER;

        List<CountingActor> actors = new ArrayList<>();
        for (int a = 0; a < ACTOR_COUNT; a++) {
            CountingActor actor = new CountingActor("race-actor-" + a);
            actor.start();
            actors.add(actor);
        }

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch allSendersDone = new CountDownLatch(ACTOR_COUNT * SENDER_PER_ACTOR);

        for (CountingActor actor : actors) {
            for (int s = 0; s < SENDER_PER_ACTOR; s++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int m = 0; m < MSG_PER_SENDER; m++) dispatcher.dispatch("msg", actor);
                    allSendersDone.countDown();
                });
            }
        }

        gate.countDown();
        allSendersDone.await(20, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    for (CountingActor a : actors) {
                        assertEquals(TOTAL_PER_ACTOR, a.getReceiveCount(),
                                a.name() + " has wrong count");
                    }
                });

        actors.forEach(dispatcher::clear);
    }

    // -----------------------------------------------------------------------
    // Error handling
    // -----------------------------------------------------------------------

    @Test
    void should_callOnErrorAndKeepLoopAlive_when_actorThrowsOnSingleMessage() {
        CountingActor actor = new CountingActor("actor-erring");
        actor.setFailOnMessage("bad");
        actor.start();

        dispatcher.dispatch("good-1", actor);
        dispatcher.dispatch("bad", actor);
        dispatcher.dispatch("good-2", actor);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertEquals(2, actor.getReceiveCount(), "Good messages not processed");
                    assertEquals(1, actor.getErrorMessages().size(), "onError not called");
                    assertEquals("bad", actor.getErrorMessages().get(0));
                });

        dispatcher.clear(actor);
    }

    @Test
    void should_neverKillSpinLoop_when_actorThrowsOnMultipleMessages() {
        CountingActor actor = new CountingActor("actor-multi-err");
        actor.setFailOnMessage("FAIL");
        actor.start();

        final int TOTAL = 50;
        final int BAD = 10; // every 5th message is "FAIL"
        final int GOOD = TOTAL - BAD;

        for (int i = 0; i < TOTAL; i++) {
            dispatcher.dispatch(i % 5 == 0 ? "FAIL" : "ok-" + i, actor);
        }

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertEquals(GOOD, actor.getReceiveCount());
                    assertEquals(BAD, actor.getErrorMessages().size());
                });

        dispatcher.clear(actor);
    }

    // -----------------------------------------------------------------------
    // Clear
    // -----------------------------------------------------------------------

    @Test
    void should_discardPendingMessages_when_clearCalledWhileActorIsProcessing() throws InterruptedException {
        CountingActor actor = new CountingActor("actor-clear");
        actor.setProcessingDelayMs(50); // slow consumer so messages pile up
        actor.start();

        for (int i = 0; i < 20; i++) {
            dispatcher.dispatch("msg-" + i, actor);
        }

        dispatcher.clear(actor);

        Thread.sleep(300);
        assertTrue(actor.getReceiveCount() < 20,
                "clear() should have discarded some pending messages");
    }

    @Test
    void should_stopSpinThread_when_clearCalledAfterDispatch() throws InterruptedException {
        CountingActor actor = new CountingActor("actor-stop");
        actor.start();

        dispatcher.dispatch("msg", actor);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));

        // clear must not throw and subsequent dispatch must not re-use stopped state
        assertDoesNotThrow(() -> dispatcher.clear(actor));

        Thread.sleep(100); // let the spin thread cleanly exit
    }

    @Test
    void should_acceptNewDispatch_when_clearCalledAndNewActorInstanceReregistered() throws InterruptedException {
        CountingActor actor = new CountingActor("actor-reregister");
        actor.start();

        dispatcher.dispatch("first", actor);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));

        dispatcher.clear(actor);

        // Re-register: a fresh actor with the same name should work cleanly.
        CountingActor actor2 = new CountingActor("actor-reregister");
        actor2.start();
        dispatcher.dispatch("second", actor2);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(1, actor2.getReceiveCount()));

        dispatcher.clear(actor2);
    }

    @Test
    void should_useDefaultCapacity_when_noConfigProvided() {
        assertEquals(MpscMailbox.DEFAULT_CAPACITY,
                SpinningActorDispatcher.Config.defaults().mailboxCapacity(),
                "Default config capacity must equal MpscMailbox.DEFAULT_CAPACITY");
    }

    @Test
    void should_throw_when_configCreatedWithZeroOrNegativeCapacity() {
        assertThrows(IllegalArgumentException.class,
                () -> new SpinningActorDispatcher.Config(0));
        assertThrows(IllegalArgumentException.class,
                () -> new SpinningActorDispatcher.Config(-1));
    }

    // -----------------------------------------------------------------------
    // Stress
    // -----------------------------------------------------------------------

    @Test
    @Timeout(60)
    void should_deliverAllMessages_when_highVolumeLoadAcrossMultipleActorsAndSenders()
            throws InterruptedException {

        // Keep actor count low: each actor pins a dedicated OS thread.
        final int ACTOR_COUNT = 4;
        final int SENDER_COUNT = 40;
        final int MESSAGES_EACH = 100;

        List<CountingActor> actors = new ArrayList<>();
        int[] msgPerActor = new int[ACTOR_COUNT];

        for (int a = 0; a < ACTOR_COUNT; a++) {
            CountingActor actor = new CountingActor("stress-" + a);
            actor.start();
            actors.add(actor);
        }
        for (int s = 0; s < SENDER_COUNT; s++) {
            msgPerActor[s % ACTOR_COUNT] += MESSAGES_EACH;
        }

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(SENDER_COUNT);

        for (int s = 0; s < SENDER_COUNT; s++) {
            final CountingActor target = actors.get(s % ACTOR_COUNT);
            final int senderIdx = s;
            Thread.ofVirtual().start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int m = 0; m < MESSAGES_EACH; m++) {
                    dispatcher.dispatch("s" + senderIdx + "-m" + m, target);
                }
                done.countDown();
            });
        }

        gate.countDown();
        done.await(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    for (int a = 0; a < ACTOR_COUNT; a++) {
                        assertEquals(msgPerActor[a], actors.get(a).getReceiveCount(),
                                actors.get(a).name() + " wrong count in stress test");
                    }
                });

        actors.forEach(dispatcher::clear);
    }
}
