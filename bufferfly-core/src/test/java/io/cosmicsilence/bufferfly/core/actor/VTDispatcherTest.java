package io.cosmicsilence.bufferfly.core.actor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VTDispatcher}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Single and multi-message delivery.</li>
 *   <li>Actor isolation — messages never cross actor boundaries.</li>
 *   <li>Burst delivery (5 000 messages through one actor).</li>
 *   <li>Concurrent senders → same actor (race condition).</li>
 *   <li>Single-writer invariant — {@code receive()} is never called concurrently.</li>
 *   <li>Concurrent senders → many actors simultaneously (race condition).</li>
 *   <li>Error handling — {@code onError} called; loop survives.</li>
 *   <li>Clear — discards pending messages without crashing.</li>
 *   <li>Stress — 30 actors × 100 virtual-thread senders × 200 messages each.</li>
 * </ul>
 * All asynchronous assertions use Awaitility.
 */
@Timeout(60)
class VTDispatcherTest {

    private VTDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new VTDispatcher();
    }

    // -----------------------------------------------------------------------
    // Basic dispatch
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessage_when_singleMessageDispatched() {
        DummyCountingActor actor = new DummyCountingActor("actor-single");
        actor.start();

        dispatcher.dispatch("hello", actor);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, actor.getReceiveCount());
                   assertEquals("hello", actor.getReceived().get(0));
               });
    }

    @Test
    void should_deliverAllMessages_when_multipleMessagesDispatchedSequentially() {
        DummyCountingActor actor = new DummyCountingActor("actor-multi");
        actor.start();

        dispatcher.dispatch("msg-1", actor);
        dispatcher.dispatch("msg-2", actor);
        dispatcher.dispatch("msg-3", actor);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(3, actor.getReceiveCount()));
    }

    // -----------------------------------------------------------------------
    // Actor isolation
    // -----------------------------------------------------------------------

    @Test
    void should_neverCrossMessages_when_twoActorsReceiveInterleavedDispatches() {
        DummyCountingActor alpha = new DummyCountingActor("actor-alpha");
        DummyCountingActor beta  = new DummyCountingActor("actor-beta");
        alpha.start();
        beta.start();

        dispatcher.dispatch("for-alpha-1", alpha);
        dispatcher.dispatch("for-beta-1",  beta);
        dispatcher.dispatch("for-alpha-2", alpha);
        dispatcher.dispatch("for-beta-2",  beta);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(2, alpha.getReceiveCount());
                   assertEquals(2, beta.getReceiveCount());
                   assertTrue(alpha.getReceived().stream().allMatch(m -> m.contains("alpha")));
                   assertTrue(beta.getReceived().stream().allMatch(m -> m.contains("beta")));
               });
    }

    @Test
    void should_deliverOnlyOwnMessages_when_manyActorsDispatchedConcurrently() {
        final int ACTOR_COUNT   = 10;
        final int MSG_PER_ACTOR = 20;

        List<DummyCountingActor> actors = new ArrayList<>();
        for (int i = 0; i < ACTOR_COUNT; i++) {
            DummyCountingActor a = new DummyCountingActor("iso-actor-" + i);
            a.start();
            actors.add(a);
        }

        for (int m = 0; m < MSG_PER_ACTOR; m++) {
            for (DummyCountingActor a : actors) {
                dispatcher.dispatch("msg-for-" + a.name(), a);
            }
        }

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   for (DummyCountingActor a : actors) {
                       assertEquals(MSG_PER_ACTOR, a.getReceiveCount(),
                           a.name() + " wrong count");
                       assertTrue(a.getReceived().stream().allMatch(msg -> msg.contains(a.name())),
                           a.name() + " received a foreign message");
                   }
               });
    }

    // -----------------------------------------------------------------------
    // Burst delivery
    // -----------------------------------------------------------------------

    @Test
    void should_deliverAllMessages_when_largeBurstDispatchedToSingleActor() {
        final int BURST = 5_000;
        DummyCountingActor actor = new DummyCountingActor("actor-burst");
        actor.start();

        for (int i = 0; i < BURST; i++) {
            dispatcher.dispatch("msg-" + i, actor);
        }

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(BURST, actor.getReceiveCount(),
                   "Not all burst messages were delivered"));
    }

    // -----------------------------------------------------------------------
    // Concurrent senders → same actor (race condition)
    // -----------------------------------------------------------------------

    @Test
    void should_loseNoMessages_when_manyConcurrentSendersTargetSameActor() throws InterruptedException {
        final int SENDER_THREADS = 50;
        final int MSG_PER_SENDER = 100;
        final int TOTAL          = SENDER_THREADS * MSG_PER_SENDER;

        DummyCountingActor actor = new DummyCountingActor("actor-concurrent");
        actor.start();

        CountDownLatch startGate   = new CountDownLatch(1);
        CountDownLatch sendersDone = new CountDownLatch(SENDER_THREADS);

        for (int t = 0; t < SENDER_THREADS; t++) {
            final int base = t * MSG_PER_SENDER;
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < MSG_PER_SENDER; i++) {
                    dispatcher.dispatch("msg-" + (base + i), actor);
                }
                sendersDone.countDown();
            });
        }

        startGate.countDown();
        sendersDone.await(20, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(TOTAL, actor.getReceiveCount(),
                   "Some messages were lost under concurrent senders"));
    }

    @Test
    void should_neverCallReceiveConcurrently_when_manySendersRaceOnSameActor() throws InterruptedException {
        final int SENDER_THREADS = 30;
        final int MSG_PER_SENDER = 50;
        final int TOTAL          = SENDER_THREADS * MSG_PER_SENDER;

        AtomicInteger concurrentReceives = new AtomicInteger(0);
        AtomicInteger activeReceivers    = new AtomicInteger(0);

        DummyCountingActor actor = new DummyCountingActor("actor-single-writer") {
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
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < MSG_PER_SENDER; i++) dispatcher.dispatch("m", actor);
                done.countDown();
            });
        }

        gate.countDown();
        done.await(20, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(TOTAL, actor.getReceiveCount()));

        assertEquals(0, concurrentReceives.get(),
            "Single-writer violation: actor.receive() was invoked concurrently");
    }

    // -----------------------------------------------------------------------
    // Concurrent senders → many actors simultaneously (race condition)
    // -----------------------------------------------------------------------

    @Test
    void should_loseNoMessages_when_manySendersTargetManyActorsSimultaneously() throws InterruptedException {
        final int ACTOR_COUNT      = 20;
        final int SENDER_PER_ACTOR = 10;
        final int MSG_PER_SENDER   = 50;
        final int TOTAL_PER_ACTOR  = SENDER_PER_ACTOR * MSG_PER_SENDER;

        List<DummyCountingActor> actors = new ArrayList<>();
        for (int a = 0; a < ACTOR_COUNT; a++) {
            DummyCountingActor actor = new DummyCountingActor("race-actor-" + a);
            actor.start();
            actors.add(actor);
        }

        CountDownLatch gate          = new CountDownLatch(1);
        CountDownLatch allSendersDone = new CountDownLatch(ACTOR_COUNT * SENDER_PER_ACTOR);

        for (DummyCountingActor actor : actors) {
            for (int s = 0; s < SENDER_PER_ACTOR; s++) {
                Thread.ofVirtual().start(() -> {
                    try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    for (int m = 0; m < MSG_PER_SENDER; m++) dispatcher.dispatch("msg", actor);
                    allSendersDone.countDown();
                });
            }
        }

        gate.countDown();
        allSendersDone.await(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   for (DummyCountingActor a : actors) {
                       assertEquals(TOTAL_PER_ACTOR, a.getReceiveCount(),
                           a.name() + " has wrong count");
                   }
               });
    }

    // -----------------------------------------------------------------------
    // Error handling
    // -----------------------------------------------------------------------

    @Test
    void should_callOnErrorAndKeepLoopAlive_when_actorThrowsOnSingleMessage() {
        DummyCountingActor actor = new DummyCountingActor("actor-erring");
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
    }

    @Test
    void should_neverKillDispatchLoop_when_actorThrowsOnMultipleMessages() {
        DummyCountingActor actor = new DummyCountingActor("actor-multi-err");
        actor.setFailOnMessage("FAIL");
        actor.start();

        // Indices 0..49: FAIL at 0,5,10,15,20,25,30,35,40,45 → 10 FAILs, 40 good
        final int TOTAL = 50;
        final int BAD   = 10;
        final int GOOD  = TOTAL - BAD;

        for (int i = 0; i < TOTAL; i++) {
            dispatcher.dispatch(i % 5 == 0 ? "FAIL" : "ok-" + i, actor);
        }

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   assertEquals(GOOD, actor.getReceiveCount());
                   assertEquals(BAD,  actor.getErrorMessages().size());
               });
    }

    // -----------------------------------------------------------------------
    // Clear
    // -----------------------------------------------------------------------

    @Test
    void should_discardPendingMessages_when_clearCalledWhileActorIsProcessing() throws InterruptedException {
        DummyCountingActor actor = new DummyCountingActor("actor-clear");
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

    // -----------------------------------------------------------------------
    // Stress
    // -----------------------------------------------------------------------

    @Test
    @Timeout(60)
    void should_deliverAllMessages_when_highVolumeLoadAcrossManyActorsAndSenders() throws InterruptedException {
        final int ACTOR_COUNT   = 30;
        final int SENDER_COUNT  = 100;
        final int MESSAGES_EACH = 200;

        List<DummyCountingActor> actors = new ArrayList<>();
        int[] msgPerActor = new int[ACTOR_COUNT];

        for (int a = 0; a < ACTOR_COUNT; a++) {
            DummyCountingActor actor = new DummyCountingActor("stress-" + a);
            actor.start();
            actors.add(actor);
        }
        for (int s = 0; s < SENDER_COUNT; s++) {
            msgPerActor[s % ACTOR_COUNT] += MESSAGES_EACH;
        }

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(SENDER_COUNT);

        for (int s = 0; s < SENDER_COUNT; s++) {
            final DummyCountingActor target = actors.get(s % ACTOR_COUNT);
            final int senderIdx = s;
            Thread.ofVirtual().start(() -> {
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
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
    }
}
