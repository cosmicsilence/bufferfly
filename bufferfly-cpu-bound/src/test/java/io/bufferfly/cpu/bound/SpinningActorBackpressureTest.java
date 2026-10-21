package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.backpressure.BackpressureActorDispatcher;
import io.bufferfly.core.backpressure.BackpressureStrategy;
import io.bufferfly.core.backpressure.UnderPressureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that the high-performance {@link SpinningActorDispatcher} supports
 * <em>only</em> the {@link BackpressureStrategy#THROW} backpressure strategy.
 *
 * <p>{@code CALLER_RUNS} would run actor work on the producer's thread, which
 * defeats the hardware-pinned spinning design, so it must be rejected outright.
 */
@Timeout(60)
class SpinningActorBackpressureTest {

    static final class CountingActor extends AbstractActor<String> {
        final AtomicInteger count = new AtomicInteger(0);
        private final String name;

        CountingActor(String name) {
            this.name = name;
        }

        @Override
        public void receive(String message) {
            count.incrementAndGet();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onError(String message, Exception e) {
        }
    }

    @Test
    void should_rejectCallerRuns_when_wrappingSpinningDispatcher() {
        SpinningActorDispatcher spinning = new SpinningActorDispatcher();
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new BackpressureActorDispatcher(spinning, BackpressureStrategy.CALLER_RUNS));
        // message should point at the capability requirement
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("CallerRunsCapable"),
                "message should explain the CallerRunsCapable requirement: " + ex.getMessage());
    }

    @Test
    void should_supportThrow_when_wrappingSpinningDispatcher() {
        SpinningActorDispatcher spinning = new SpinningActorDispatcher();
        var bp = new BackpressureActorDispatcher(spinning, BackpressureStrategy.THROW);
        CountingActor actor = new CountingActor("throw-ok");
        actor.start();

        // Not under pressure: dispatch flows through to the spinning dispatcher.
        bp.dispatch("m1", actor);
        bp.dispatch("m2", actor);
        await().untilAsserted(() -> assertEquals(2, actor.count.get()));

        // Under pressure: dispatch is rejected fast, nothing new is delivered.
        bp.applyPressure();
        assertThrows(UnderPressureException.class, () -> bp.dispatch("m3", actor));
        assertEquals(2, actor.count.get(), "no new message delivered while under pressure");

        // Pressure released: dispatch resumes.
        bp.releasePressure();
        bp.dispatch("m4", actor);
        await().untilAsserted(() -> assertEquals(3, actor.count.get()));

        spinning.clear(actor);
    }
}
