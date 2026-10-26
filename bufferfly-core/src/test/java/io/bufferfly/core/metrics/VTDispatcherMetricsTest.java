package io.bufferfly.core.metrics;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorContext;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.actor.VTDispatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the mailbox/dispatch collectors fire on the real {@link VTDispatcher}
 * hot path when an {@link InMemoryBufferflyMetrics} is wired in:
 * <ul>
 *   <li>every accepted message increments the per-actor enqueue counter;</li>
 *   <li>a refusal (full bounded mailbox) increments the refuse counter and
 *       throws {@code MailBoxRefusedException};</li>
 *   <li>a mailbox-size gauge is registered and reports the live depth.</li>
 * </ul>
 */
@Timeout(30)
class VTDispatcherMetricsTest {

    /** A minimal actor: counts receives; can be told to block in receive() to hold the mailbox full. */
    static final class CountingActor implements Actor<String> {
        private final String name;
        private final AtomicInteger received = new AtomicInteger();
        private final CountDownLatch gate; // if non-null, receive() blocks on it

        CountingActor(String name, CountDownLatch gate) {
            this.name = name;
            this.gate = gate;
        }

        @Override public void receive(String message) {
            if (gate != null) {
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            received.incrementAndGet();
        }
        @Override public void setContext(ActorContext<String> context) { }
        @Override public String name() { return name; }
        @Override public void onError(String message, Exception e) { }
        @Override public void start() { }
        @Override public void stop() { }
        @Override public State currentState() { return State.STARTED; }

        int receiveCount() { return received.get(); }
    }

    @Test
    void should_countEnqueues_and_registerMailboxSizeGauge() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        VTDispatcher dispatcher = new VTDispatcher(1_000, Map.of(), metrics);
        CountingActor actor = new CountingActor("metrics-actor", null);

        final int n = 50;
        for (int i = 0; i < n; i++) {
            dispatcher.dispatch("m" + i, actor);
        }

        // All messages are accepted into the mailbox and drained by the actor.
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(n, actor.receiveCount()));

        assertEquals(n, metrics.mailboxEnqueuedFor("metrics-actor"),
                "every accepted message must increment the per-actor enqueue counter");
        assertEquals(n, metrics.mailboxEnqueuedTotal(), "aggregate enqueue counter matches");
        assertEquals(0, metrics.mailboxRefusedTotal(), "nothing should be refused on a roomy mailbox");

        // A mailbox-size gauge was registered for the actor and samples the live depth.
        assertTrue(metrics.hasMailboxSizeGauge("metrics-actor"), "a mailbox-size gauge must be registered");
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(0, metrics.mailboxSize("metrics-actor"),
                        "the drained mailbox depth must settle to 0"));

        dispatcher.stop();
    }

    @Test
    void should_countRefusal_when_boundedMailboxIsFull() throws InterruptedException {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        // Capacity 1: the first message is taken by the drain loop and blocks in
        // receive(); the next fills the single slot; the third is refused.
        VTDispatcher dispatcher = new VTDispatcher(1, Map.of(), metrics);
        CountDownLatch gate = new CountDownLatch(1);
        CountingActor actor = new CountingActor("tiny-mailbox", gate);

        try {
            // 1st: accepted, drained, blocks in receive() on the gate.
            dispatcher.dispatch("a", actor);
            // Give the drain loop a moment to pick up the first message and block.
            await().atMost(java.time.Duration.ofSeconds(5))
                    .until(() -> metrics.mailboxEnqueuedFor("tiny-mailbox") >= 1);

            // 2nd: accepted into the now-free single slot (not yet drained).
            dispatcher.dispatch("b", actor);

            // 3rd: the slot is full and the consumer is blocked → refusal.
            // BlockingMailbox.offer waits up to 500ms then returns false.
            MailBoxRefusedException ex = assertThrows(MailBoxRefusedException.class,
                    () -> dispatcher.dispatch("c", actor));
            assertTrue(ex.getMessage().contains("tiny-mailbox"));

            assertEquals(1, metrics.mailboxRefusedFor("tiny-mailbox"),
                    "the refused message must increment the refuse counter exactly once");
            assertEquals(1, metrics.mailboxRefusedTotal(), "aggregate refuse counter matches");
            assertEquals(2, metrics.mailboxEnqueuedFor("tiny-mailbox"),
                    "only the two accepted messages count as enqueued");
        } finally {
            gate.countDown(); // release the blocked actor so the drain loop can finish
            dispatcher.stop();
        }
    }

    @Test
    void should_notThrow_and_recordNothing_when_usingNoopMetrics() {
        VTDispatcher dispatcher = new VTDispatcher(); // NOOP metrics by default
        CountingActor actor = new CountingActor("noop-actor", null);
        for (int i = 0; i < 10; i++) dispatcher.dispatch("x" + i, actor);
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(10, actor.receiveCount()));
        dispatcher.stop();
    }
}
