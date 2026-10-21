package io.bufferfly.core.backpressure;

import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.core.persistence.PersistenceRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BackpressurePersistenceDispatcher} watermark edge-triggering.
 */
@Timeout(30)
class BackpressurePersistenceDispatcherTest {

    /** A fake dispatcher whose reported size we drive manually. */
    static final class FakeDispatcher implements PersistenceDispatcher<String> {
        volatile int size = 0;
        final AtomicInteger enqueueCount = new AtomicInteger(0);
        boolean cleared = false;

        @Override
        public void enqueue(PersistenceRunner.PersistenceEvent<String> event, PersistenceRunner<String> runner) {
            enqueueCount.incrementAndGet();
        }

        @Override
        public void clear() {
            cleared = true;
        }

        @Override
        public int size() {
            return size;
        }
    }

    static final class CountingCallback implements BackpressureCallback {
        final AtomicInteger onPressure = new AtomicInteger(0);
        final AtomicInteger onReleased = new AtomicInteger(0);

        @Override
        public void onBackpressure() {
            onPressure.incrementAndGet();
        }

        @Override
        public void onReleasedPressure() {
            onReleased.incrementAndGet();
        }
    }

    private static PersistenceRunner.PersistenceEvent<String> event() {
        return PersistenceRunner.newEvent("x");
    }

    @Test
    void should_fireOnBackpressure_when_sizeReachesHighWatermark() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        fake.size = 9;
        bp.enqueue(event(), null);
        assertEquals(0, cb.onPressure.get(), "below high watermark: no signal");
        assertFalse(bp.isUnderPressure());

        fake.size = 10;
        bp.enqueue(event(), null);
        assertEquals(1, cb.onPressure.get(), "reached high watermark: signal once");
        assertTrue(bp.isUnderPressure());
    }

    @Test
    void should_fireOnBackpressureOnlyOnce_when_sizeStaysAboveHighWatermark() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        fake.size = 15;
        bp.enqueue(event(), null);
        bp.enqueue(event(), null);
        bp.enqueue(event(), null);

        assertEquals(1, cb.onPressure.get(), "rising edge fires exactly once");
    }

    @Test
    void should_fireOnReleasedPressure_when_sizeRecedesToLowWatermark() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        // go over high watermark first
        fake.size = 12;
        bp.enqueue(event(), null);
        assertTrue(bp.isUnderPressure());
        assertEquals(1, cb.onPressure.get());

        // still above low watermark -> no release yet
        fake.size = 5;
        bp.enqueue(event(), null);
        assertEquals(0, cb.onReleased.get());
        assertTrue(bp.isUnderPressure());

        // recede to low watermark -> release
        fake.size = 2;
        bp.enqueue(event(), null);
        assertEquals(1, cb.onReleased.get());
        assertFalse(bp.isUnderPressure());
    }

    @Test
    void should_notReleaseTwice_when_sizeStaysLow() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        fake.size = 11;
        bp.enqueue(event(), null);
        fake.size = 1;
        bp.enqueue(event(), null);
        bp.enqueue(event(), null);

        assertEquals(1, cb.onReleased.get(), "falling edge fires exactly once");
    }

    @Test
    void should_hysteresis_when_oscillatingBetweenWatermarks() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        // rise
        fake.size = 10;
        bp.enqueue(event(), null);
        // oscillate in the dead-band (between low and high) - no new edges
        fake.size = 5;
        bp.enqueue(event(), null);
        fake.size = 8;
        bp.enqueue(event(), null);
        fake.size = 3;
        bp.enqueue(event(), null);

        assertEquals(1, cb.onPressure.get());
        assertEquals(0, cb.onReleased.get(), "no release while above low watermark");
        assertTrue(bp.isUnderPressure());
    }

    @Test
    void should_releaseOnClear_when_underPressure() {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb);

        fake.size = 20;
        bp.enqueue(event(), null);
        assertTrue(bp.isUnderPressure());

        bp.clear();
        assertTrue(fake.cleared);
        assertEquals(1, cb.onReleased.get());
        assertFalse(bp.isUnderPressure());
    }

    @Test
    void should_forwardEnqueueAndSize_toDelegate() {
        FakeDispatcher fake = new FakeDispatcher();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, BackpressureCallback.NO_OP);

        bp.enqueue(event(), null);
        bp.enqueue(event(), null);
        assertEquals(2, fake.enqueueCount.get());

        fake.size = 7;
        assertEquals(7, bp.size());
    }

    @Test
    void should_rejectInvalidWatermarks() {
        FakeDispatcher fake = new FakeDispatcher();
        assertThrows(IllegalArgumentException.class,
                () -> new BackpressurePersistenceDispatcher<>(fake, 5, 5, BackpressureCallback.NO_OP));
        assertThrows(IllegalArgumentException.class,
                () -> new BackpressurePersistenceDispatcher<>(fake, 2, 5, BackpressureCallback.NO_OP));
        assertThrows(IllegalArgumentException.class,
                () -> new BackpressurePersistenceDispatcher<>(fake, 10, -1, BackpressureCallback.NO_OP));
    }

    // -- periodic evaluator (scheduler) -------------------------------------

    /**
     * The whole point of the scheduler: after the high watermark engages
     * backpressure, upstream stops enqueueing — so without a periodic evaluator
     * the buffer draining back below the low watermark would NEVER be observed
     * and the falling edge (release) would never fire. With a configured tick,
     * the release fires on a scheduler tick even though no further enqueue
     * happens.
     */
    @Test
    void should_fireReleaseOnSchedulerTick_when_bufferDrainsWithoutEnqueue() throws InterruptedException {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        // tiny 5 ms tick for a fast, deterministic test window
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb, 5L);
        try {
            // engage pressure via an enqueue at/above the high watermark
            fake.size = 12;
            bp.enqueue(event(), null);
            assertTrue(bp.isUnderPressure());
            assertEquals(1, cb.onPressure.get());

            // now the buffer drains below the low watermark WITHOUT any enqueue;
            // only the periodic evaluator can observe it and release.
            fake.size = 1;

            long deadline = System.currentTimeMillis() + 2_000L;
            while (cb.onReleased.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }

            assertEquals(1, cb.onReleased.get(),
                    "the periodic evaluator must fire the falling edge as the buffer drains");
            assertFalse(bp.isUnderPressure());
        } finally {
            bp.clear(); // shuts the scheduler down
        }
    }

    @Test
    void should_notFireReleaseOnTick_when_bufferStaysAboveLowWatermark() throws InterruptedException {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb, 5L);
        try {
            fake.size = 12;
            bp.enqueue(event(), null);
            assertTrue(bp.isUnderPressure());

            // stays in the dead-band (above low) — ticks must NOT release
            fake.size = 5;
            Thread.sleep(100); // many ticks elapse

            assertEquals(0, cb.onReleased.get(), "no release while above the low watermark");
            assertTrue(bp.isUnderPressure());
        } finally {
            bp.clear();
        }
    }

    @Test
    void should_shutDownScheduler_onClear() throws InterruptedException {
        FakeDispatcher fake = new FakeDispatcher();
        CountingCallback cb = new CountingCallback();
        var bp = new BackpressurePersistenceDispatcher<>(fake, 10, 2, cb, 5L);

        fake.size = 12;
        bp.enqueue(event(), null);
        assertTrue(bp.isUnderPressure());

        bp.clear(); // cancels the task, shuts the scheduler down, releases pressure
        assertEquals(1, cb.onReleased.get(), "clear releases pressure");
        assertFalse(bp.isUnderPressure());

        // after clear the evaluator is gone: dropping size low and waiting must
        // NOT fire another release (the scheduler is dead, and we are no longer
        // under pressure anyway).
        int releasedAfterClear = cb.onReleased.get();
        fake.size = 0;
        Thread.sleep(50);
        assertEquals(releasedAfterClear, cb.onReleased.get(),
                "no further callbacks after the scheduler is shut down");
    }
}
