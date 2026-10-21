package io.bufferfly.core.backpressure;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorDispatcher;
import io.bufferfly.core.actor.VTDispatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BackpressureActorDispatcher} strategy switching.
 */
@Timeout(30)
class BackpressureActorDispatcherTest {

    /** Records dispatches without running the actor. Stands in for an async dispatcher (THROW tests). */
    static final class RecordingDelegate implements ActorDispatcher {
        final AtomicInteger dispatched = new AtomicInteger(0);
        final AtomicInteger cleared = new AtomicInteger(0);
        final AtomicInteger stopped = new AtomicInteger(0);

        @Override
        public <T> void dispatch(T t, Actor<T> actor) {
            dispatched.incrementAndGet();
        }

        @Override
        public <T> void clear(Actor<T> actor) {
            cleared.incrementAndGet();
        }

        @Override
        public void stop() {
            stopped.incrementAndGet();
        }
    }

    static final class RecordingActor extends AbstractActor<String> {
        final List<String> received = new CopyOnWriteArrayList<>();
        final List<Thread> threads = new CopyOnWriteArrayList<>();

        RecordingActor() {
            start(); // move to STARTED so VTDispatcher drains immediately
        }

        @Override
        public void receive(String message) {
            received.add(message);
            threads.add(Thread.currentThread());
        }

        @Override
        public String name() {
            return "a";
        }

        @Override
        public void onError(String message, Exception e) {
        }
    }

    // ---- THROW strategy (delegate need not be CallerRunsCapable) ----

    @Test
    void should_forwardToDelegate_when_notUnderPressure_throwStrategy() {
        RecordingDelegate delegate = new RecordingDelegate();
        var bp = new BackpressureActorDispatcher(delegate, BackpressureStrategy.THROW);
        RecordingActor actor = new RecordingActor();

        bp.dispatch("m1", actor);
        bp.dispatch("m2", actor);

        assertEquals(2, delegate.dispatched.get());
    }

    @Test
    void should_throw_when_underPressureAndThrowStrategy() {
        RecordingDelegate delegate = new RecordingDelegate();
        var bp = new BackpressureActorDispatcher(delegate, BackpressureStrategy.THROW);
        RecordingActor actor = new RecordingActor();

        bp.applyPressure();
        assertThrows(UnderPressureException.class, () -> bp.dispatch("m1", actor));
        assertEquals(0, delegate.dispatched.get());
    }

    @Test
    void should_resumeDelegate_when_pressureReleased_throwStrategy() {
        RecordingDelegate delegate = new RecordingDelegate();
        var bp = new BackpressureActorDispatcher(delegate, BackpressureStrategy.THROW);
        RecordingActor actor = new RecordingActor();

        bp.applyPressure();
        assertThrows(UnderPressureException.class, () -> bp.dispatch("x", actor));

        bp.releasePressure();
        bp.dispatch("y", actor);
        assertEquals(1, delegate.dispatched.get());
    }

    @Test
    void should_forwardClearAndStop_toDelegate() {
        RecordingDelegate delegate = new RecordingDelegate();
        var bp = new BackpressureActorDispatcher(delegate, BackpressureStrategy.THROW);
        RecordingActor actor = new RecordingActor();

        bp.clear(actor);
        bp.stop();

        assertEquals(1, delegate.cleared.get());
        assertEquals(1, delegate.stopped.get());
    }

    // ---- CALLER_RUNS strategy (delegate must be CallerRunsCapable) ----

    @Test
    void should_requireCallerRunsCapableDelegate_forCallerRunsStrategy() {
        RecordingDelegate delegate = new RecordingDelegate();
        assertThrows(IllegalArgumentException.class,
                () -> new BackpressureActorDispatcher(delegate, BackpressureStrategy.CALLER_RUNS));
    }

    @Test
    void should_toggleDelegateMode_onPressureTransitions() {
        VTDispatcher vt = new VTDispatcher();
        var bp = new BackpressureActorDispatcher(vt, BackpressureStrategy.CALLER_RUNS);

        assertTrue(!vt.isCallerRuns());
        bp.applyPressure();
        assertTrue(vt.isCallerRuns(), "delegate flipped to caller-runs under pressure");
        bp.releasePressure();
        assertTrue(!vt.isCallerRuns(), "delegate restored to async on release");

        vt.stop();
    }

    @Test
    void should_runOnCallerThread_when_underPressureAndCallerRuns() {
        VTDispatcher vt = new VTDispatcher();
        var bp = new BackpressureActorDispatcher(vt, BackpressureStrategy.CALLER_RUNS);
        RecordingActor actor = new RecordingActor();

        bp.applyPressure();
        Thread caller = Thread.currentThread();
        bp.dispatch("m1", actor);

        // Inline execution: by the time dispatch returns the message is processed.
        assertEquals(List.of("m1"), actor.received);
        assertEquals(caller, actor.threads.get(0),
                "under caller-runs the actor runs on the producer thread");

        vt.stop();
    }

    @Test
    void should_preserveOrder_when_switchingFromAsyncToCallerRuns() throws Exception {
        VTDispatcher vt = new VTDispatcher();
        var bp = new BackpressureActorDispatcher(vt, BackpressureStrategy.CALLER_RUNS);

        // A slow actor so that early async messages are still queued when we flip.
        var actor = new AbstractActor<Integer>() {
            final List<Integer> received = new CopyOnWriteArrayList<>();
            {
                start();
            }

            @Override
            public void receive(Integer message) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                received.add(message);
            }

            @Override
            public String name() {
                return "ordered";
            }

            @Override
            public void onError(Integer message, Exception e) {
            }
        };

        final int total = 200;
        for (int i = 0; i < total; i++) {
            if (i == 50) bp.applyPressure(); // flip mid-stream
            bp.dispatch(i, actor);
        }

        await().until(() -> actor.received.size() == total);

        // Must be strictly increasing 0..total-1 — no reordering across the flip.
        for (int i = 0; i < total; i++) {
            assertEquals(i, actor.received.get(i),
                    "message order must be preserved across async->caller-runs switch");
        }

        vt.stop();
    }

    @Test
    void should_exposeStrategy() {
        VTDispatcher vt = new VTDispatcher();
        var bp = new BackpressureActorDispatcher(vt, BackpressureStrategy.CALLER_RUNS);
        assertSame(BackpressureStrategy.CALLER_RUNS, bp.strategy());
        vt.stop();
    }
}
