package io.bufferfly.core.actor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the actor {@link Scheduler} capability: an actor schedules a message to
 * itself after a delay, and the framework delivers it exactly once on the
 * actor's own thread.
 */
@Timeout(30)
class SchedulerTest {

    /**
     * On start, schedules a "tick" to self after a delay; records every message
     * it receives with the time it was received so the test can assert the delay
     * elapsed and the typed message arrived.
     */
    static final class SchedulingActor extends AbstractActor<String> {
        final List<String> received = new CopyOnWriteArrayList<>();
        volatile long scheduledAtMs;
        volatile long receivedTickAtMs;
        private final long delayMs;

        SchedulingActor(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        protected void preStart() {
            scheduledAtMs = System.currentTimeMillis();
            context.schedule("tick", Duration.ofMillis(delayMs));
        }

        @Override
        public void receive(String message) {
            received.add(message);
            if ("tick".equals(message)) {
                receivedTickAtMs = System.currentTimeMillis();
            }
        }

        @Override
        public String name() {
            return "scheduling-actor";
        }

        @Override
        public void onError(String message, Exception e) {
        }
    }

    @Test
    void should_deliverScheduledMessageToSelf_afterTheDelay() {
        ActorManager manager = new ActorManager();
        try {
            SchedulingActor actor = new SchedulingActor(150);
            manager.start(actor);

            await().untilAsserted(() -> assertEquals(List.of("tick"), actor.received,
                    "the scheduled message must be delivered to self exactly once"));

            long elapsed = actor.receivedTickAtMs - actor.scheduledAtMs;
            assertTrue(elapsed >= 150,
                    "the message must arrive only AFTER the delay (elapsed=" + elapsed + "ms)");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void should_deliverMultipleIndependentTimers_inDelayOrder() {
        ActorManager manager = new ActorManager();
        try {
            class MultiTimerActor extends AbstractActor<String> {
                final List<String> order = new CopyOnWriteArrayList<>();

                @Override
                protected void preStart() {
                    // scheduled out of order; must arrive in delay order
                    context.schedule("late", Duration.ofMillis(250));
                    context.schedule("early", Duration.ofMillis(60));
                }

                @Override
                public void receive(String message) {
                    order.add(message);
                }

                @Override
                public String name() {
                    return "multi-timer";
                }

                @Override
                public void onError(String message, Exception e) {
                }
            }
            MultiTimerActor actor = new MultiTimerActor();
            manager.start(actor);

            await().untilAsserted(() -> assertEquals(List.of("early", "late"), actor.order,
                    "independent one-shot timers fire in delay order, each exactly once"));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void should_notDeliver_whenActorStoppedBeforeDelayElapses() {
        ActorManager manager = new ActorManager();
        try {
            class SlowTimerActor extends AbstractActor<String> {
                final List<String> received = new CopyOnWriteArrayList<>();

                @Override
                protected void preStart() {
                    context.schedule("tick", Duration.ofMillis(300));
                }

                @Override
                public void receive(String message) {
                    received.add(message);
                }

                @Override
                public String name() {
                    return "slow-timer";
                }

                @Override
                public void onError(String message, Exception e) {
                }
            }
            SlowTimerActor actor = new SlowTimerActor();
            ActorReference<String> ref = manager.start(actor);
            ref.stop(); // stop before the 300ms timer fires

            // wait past the delay; the stopped actor must NOT receive the tick
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertTrue(actor.received.isEmpty(),
                    "a timer that fires after stop must be silently dropped");
        } finally {
            manager.shutdown();
        }
    }
}
