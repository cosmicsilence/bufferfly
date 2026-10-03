package io.cosmicsilence.bufferfly.core.actor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link ActorManager}.
 *
 * <p>Exercises the full delivery stack:
 * {@code ActorManager.start()} → {@code ActorReference.tell()} →
 * {@code VTDispatcher.dispatch()} → {@code BlockingMailbox} → {@code Actor.receive()}.
 *
 * <p>Scenarios:
 * <ol>
 *   <li>Single actor tell — message arrives end-to-end.</li>
 *   <li>Two different actors — messages never cross over.</li>
 *   <li>Starting the same actor twice is idempotent (same reference).</li>
 *   <li>Duplicate name — second registration is ignored, only first receives.</li>
 *   <li>Concurrent start — only one actor instance is initialised.</li>
 *   <li>Stop then re-start with a fresh instance under the same name.</li>
 *   <li>Concurrent tell from 80 virtual-thread senders — no message lost.</li>
 *   <li>Error in receive — loop continues, {@code onError} is called.</li>
 *   <li>Fleet of 25 actors — perfect message isolation.</li>
 *   <li>Stop and re-start cycle repeated three times.</li>
 *   <li>Tell is not reachable before start — compile-time guarantee via type separation.</li>
 *   <li>End-to-end burst of 3 000 messages through the full stack.</li>
 * </ol>
 * All asynchronous assertions use Awaitility.
 */
@Timeout(60)
class ActorManagerTest {

    private ActorManager manager;

    @BeforeEach
    void setUp() {
        manager = new ActorManager();
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — single actor tell
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessage_when_singleActorReceivesTell() {
        DummyCountingActor actor = new DummyCountingActor("mgr-single");

        ActorReference<String> ref = manager.start(actor);
        ref.tell("hello");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(1, actor.getReceiveCount());
                   assertEquals("hello", actor.getReceived().get(0));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — two actors, no cross-over
    // -----------------------------------------------------------------------

    @Test
    void should_keepMessagesIsolated_when_twoActorsReceiveTellsInterleaved() {
        DummyCountingActor actorA = new DummyCountingActor("mgr-actor-A");
        DummyCountingActor actorB = new DummyCountingActor("mgr-actor-B");

        ActorReference<String> refA = manager.start(actorA);
        ActorReference<String> refB = manager.start(actorB);

        refA.tell("A-msg-1"); refB.tell("B-msg-1");
        refA.tell("A-msg-2"); refB.tell("B-msg-2");
        refA.tell("A-msg-3"); refB.tell("B-msg-3");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(3, actorA.getReceiveCount());
                   assertEquals(3, actorB.getReceiveCount());
                   assertTrue(actorA.getReceived().stream().allMatch(m -> m.startsWith("A-")));
                   assertTrue(actorB.getReceived().stream().allMatch(m -> m.startsWith("B-")));
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 3 — idempotent start
    // -----------------------------------------------------------------------

    @Test
    void should_routeBothTellsToSameInstance_when_sameActorStartedTwice() {
        DummyCountingActor actor = new DummyCountingActor("mgr-idempotent");

        ActorReference<String> ref1 = manager.start(actor);
        ActorReference<String> ref2 = manager.start(actor);

        ref1.tell("from-ref1");
        ref2.tell("from-ref2");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(2, actor.getReceiveCount()));
    }

    // -----------------------------------------------------------------------
    // Scenario 4 — duplicate name: second registration ignored
    // -----------------------------------------------------------------------

    @Test
    void should_ignoreSecondRegistration_when_actorWithSameNameAlreadyStarted() {
        DummyCountingActor first  = new DummyCountingActor("same-name");
        DummyCountingActor second = new DummyCountingActor("same-name");

        ActorReference<String> ref = manager.start(first);
        manager.start(second); // duplicate — must be silently ignored

        ref.tell("ping");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, first.getReceiveCount()));

        assertEquals(0, second.getReceiveCount(),
            "Duplicate-name actor must not receive any messages");
    }

    // -----------------------------------------------------------------------
    // Scenario 5 — concurrent start: only one instance created
    // -----------------------------------------------------------------------

    @Test
    void should_initialiseActorExactlyOnce_when_startCalledConcurrentlyFromManyThreads() throws InterruptedException {
        final int THREADS = 50;
        AtomicInteger startCount = new AtomicInteger(0);

        DummyCountingActor actor = new DummyCountingActor("concurrent-start") {
            @Override
            protected void preStart() {
                startCount.incrementAndGet();
            }
        };

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        Set<ActorReference<String>> refs = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < THREADS; t++) {
            Thread.ofVirtual().start(() -> {
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                refs.add(manager.start(actor));
                done.countDown();
            });
        }

        gate.countDown();
        done.await(10, TimeUnit.SECONDS);

        assertEquals(1, startCount.get(),
            "preStart() must be invoked exactly once despite concurrent starts");
    }

    // -----------------------------------------------------------------------
    // Scenario 6 — stop then re-start with a fresh instance under the same name
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessageToNewInstance_when_actorStoppedAndRestartedUnderSameName() {
        DummyCountingActor first = new DummyCountingActor("mgr-stop-restart");
        ActorReference<String> ref = manager.start(first);
        ref.tell("round-1");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, first.getReceiveCount()));

        ref.stop(); // removes "mgr-stop-restart" from registry

        // Re-register a fresh actor under the SAME name.
        // BUG: VTDispatcher.stop() empties the mailbox but does not terminate
        // the in-flight virtual thread from the previous consume() loop.
        // That stale VT thread still holds a reference to `first` (the old
        // delegate) and will steal messages from the shared mailbox, so
        // `second` never receives them.
        DummyCountingActor second = new DummyCountingActor("mgr-stop-restart");
        ActorReference<String> ref2 = manager.start(second);
        ref2.tell("round-2");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, second.getReceiveCount()));

        assertEquals(1, first.getReceiveCount(), "First instance must not get second message");
    }

    // -----------------------------------------------------------------------
    // Scenario 7 — concurrent tell from many virtual threads
    // -----------------------------------------------------------------------

    @Test
    void should_deliverAllMessages_when_manyVirtualThreadsSendConcurrently() throws InterruptedException {
        final int SENDER_THREADS = 80;
        final int MSG_PER_SENDER = 100;
        final int TOTAL          = SENDER_THREADS * MSG_PER_SENDER;

        DummyCountingActor actor = new DummyCountingActor("mgr-concurrent-tell");
        ActorReference<String> ref = manager.start(actor);

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(SENDER_THREADS);

        for (int t = 0; t < SENDER_THREADS; t++) {
            final int base = t * MSG_PER_SENDER;
            Thread.ofVirtual().start(() -> {
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int m = 0; m < MSG_PER_SENDER; m++) ref.tell("msg-" + (base + m));
                done.countDown();
            });
        }

        gate.countDown();
        done.await(20, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(TOTAL, actor.getReceiveCount(),
                   "Some messages were lost under concurrent tell"));
    }

    // -----------------------------------------------------------------------
    // Scenario 8 — error in receive: loop continues, onError called
    // -----------------------------------------------------------------------

    @Test
    void should_invokeOnErrorAndContinueProcessing_when_actorThrowsDuringReceive() {
        DummyCountingActor actor = new DummyCountingActor("mgr-error");
        actor.setFailOnMessage("BOOM");
        ActorReference<String> ref = manager.start(actor);

        ref.tell("ok-1");
        ref.tell("BOOM");
        ref.tell("ok-2");
        ref.tell("BOOM");
        ref.tell("ok-3");
        ref.tell("ok-4");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   assertEquals(4, actor.getReceiveCount(), "Good messages not all processed");
                   assertEquals(2, actor.getErrorMessages().size(), "onError count wrong");
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 9 — fleet of 25 actors: perfect isolation
    // -----------------------------------------------------------------------

    @Test
    void should_deliverOnlyOwnMessages_when_largeFleetOfActorsReceivesMessages() {
        final int FLEET_SIZE    = 25;
        final int MSG_PER_ACTOR = 40;

        List<DummyCountingActor>     fleet = new ArrayList<>();
        List<ActorReference<String>> refs  = new ArrayList<>();

        for (int i = 0; i < FLEET_SIZE; i++) {
            DummyCountingActor a = new DummyCountingActor("fleet-" + i);
            fleet.add(a);
            refs.add(manager.start(a));
        }

        for (int m = 0; m < MSG_PER_ACTOR; m++) {
            for (int i = 0; i < FLEET_SIZE; i++) {
                refs.get(i).tell("for-fleet-" + i + "-round-" + m);
            }
        }

        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> {
                   for (int i = 0; i < FLEET_SIZE; i++) {
                       DummyCountingActor a = fleet.get(i);
                       final int idx = i;
                       assertEquals(MSG_PER_ACTOR, a.getReceiveCount(),
                           a.name() + " wrong count");
                       assertTrue(a.getReceived().stream().allMatch(msg -> msg.contains("fleet-" + idx + "-")),
                           a.name() + " received a foreign message");
                   }
               });
    }

    // -----------------------------------------------------------------------
    // Scenario 10 — stop/restart cycle three times under the same name
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessagesToNewInstance_when_actorIsStoppedAndRestartedRepeatedly() {
        // Each cycle stops and re-registers under the SAME name.
        // BUG: the stale consume() VT thread from the previous cycle can
        // steal messages from the new actor's mailbox, causing under-delivery.
        for (int cycle = 1; cycle <= 3; cycle++) {
            DummyCountingActor actor = new DummyCountingActor("mgr-cycle");
            ActorReference<String> ref = manager.start(actor);

            for (int m = 0; m < 5; m++) ref.tell("round-" + cycle + "-msg-" + m);

            final int expectedCycle = cycle;
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertEquals(5, actor.getReceiveCount(),
                       "Cycle " + expectedCycle + " wrong count"));

            ref.stop();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 11 — tell() is only reachable via ActorReference (compile-time guarantee)
    // -----------------------------------------------------------------------

    @Test
    void should_notExposeDirectTell_when_actorIsUsedWithoutManager() {
        // Actor no longer extends ActorReference, so tell() does not exist on
        // DummyCountingActor at all — this is a compile-time guarantee, not a
        // runtime one. Here we verify the only way to reach tell() is through
        // the ActorReference returned by ActorManager.start(), which always
        // calls delegate.start() before returning, making the pre-start state
        // unreachable for callers.
        DummyCountingActor actor = new DummyCountingActor("not-started");
        // actor.tell("msg");  // would not compile — tell() does not exist on Actor
        // The actor itself has no tell(); only the ActorReference wrapper does.
        assertFalse(actor.isStarted(), "Actor must not be started before manager.start()");
    }

    // -----------------------------------------------------------------------
    // Scenario 12 — end-to-end burst through full stack
    // -----------------------------------------------------------------------

    @Test
    void should_deliverAllMessages_when_burstSentThroughFullStack() {
        final int BURST = 3_000;
        DummyCountingActor actor = new DummyCountingActor("mgr-burst");
        ActorReference<String> ref = manager.start(actor);

        for (int i = 0; i < BURST; i++) ref.tell("burst-" + i);

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertEquals(BURST, actor.getReceiveCount(),
                   "End-to-end burst not fully delivered"));
    }
}
