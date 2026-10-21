package io.bufferfly.core.backpressure;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorDispatcher;
import io.bufferfly.core.actor.VTDispatcher;
import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.core.persistence.PersistenceRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test: persistence-layer watermark crossing cascades through
 * {@link CascadingBackpressureCallback} into a
 * {@link BackpressureActorDispatcher}, altering actor dispatch behavior.
 */
@Timeout(30)
class CascadingBackpressureTest {

    static final class FakeDispatcher implements PersistenceDispatcher<String> {
        volatile int size = 0;

        @Override
        public void enqueue(PersistenceRunner.PersistenceEvent<String> event, PersistenceRunner<String> runner) {
        }

        @Override
        public void clear() {
        }

        @Override
        public int size() {
            return size;
        }
    }

    static final class RecordingDelegate implements ActorDispatcher {
        final AtomicInteger dispatched = new AtomicInteger(0);

        @Override
        public <T> void dispatch(T t, Actor<T> actor) {
            dispatched.incrementAndGet();
        }

        @Override
        public <T> void clear(Actor<T> actor) {
        }

        @Override
        public void stop() {
        }
    }

    static final class RecordingActor extends AbstractActor<String> {
        final List<String> received = new CopyOnWriteArrayList<>();
        final List<Thread> threads = new CopyOnWriteArrayList<>();

        RecordingActor() {
            start();
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

    private static PersistenceRunner.PersistenceEvent<String> event() {
        return PersistenceRunner.newEvent("x");
    }

    @Test
    void should_cascadeCallerRuns_when_persistenceSaturatesThenDrains() {
        FakeDispatcher persistence = new FakeDispatcher();
        VTDispatcher vt = new VTDispatcher();
        var actorBp = new BackpressureActorDispatcher(vt, BackpressureStrategy.CALLER_RUNS);
        var callback = new CascadingBackpressureCallback(actorBp);
        var persistenceBp = new BackpressurePersistenceDispatcher<>(persistence, 100, 20, callback);

        RecordingActor actor = new RecordingActor();

        // Normal: async (virtual thread) drains.
        actorBp.dispatch("before", actor);
        await().until(() -> actor.received.contains("before"));
        assertFalse(vt.isCallerRuns());

        // Persistence buffer saturates -> cascade pressure into the actor dispatcher.
        persistence.size = 100;
        persistenceBp.enqueue(event(), null);
        assertTrue(actorBp.isUnderPressure());
        assertTrue(vt.isCallerRuns(), "VT dispatcher flipped to caller-runs");

        // Now dispatch under pressure: the message is drained through the same
        // mailbox (ordering preserved). It runs inline on the caller unless the
        // VT that processed "before" still owns the drain slot — either way the
        // single-consumer invariant holds. (The dedicated caller-thread
        // assertion lives in BackpressureActorDispatcherTest, with no prior
        // async message to race against.)
        actorBp.dispatch("during", actor);
        await().until(() -> actor.received.contains("during"));

        // Persistence drains -> release.
        persistence.size = 10;
        persistenceBp.enqueue(event(), null);
        assertFalse(actorBp.isUnderPressure());
        assertFalse(vt.isCallerRuns(), "VT dispatcher restored to async");

        vt.stop();
    }

    @Test
    void should_cascadeThrow_when_persistenceSaturated() {
        FakeDispatcher persistence = new FakeDispatcher();
        RecordingDelegate delegate = new RecordingDelegate();
        var actorBp = new BackpressureActorDispatcher(delegate, BackpressureStrategy.THROW);
        var callback = new CascadingBackpressureCallback(actorBp);
        var persistenceBp = new BackpressurePersistenceDispatcher<>(persistence, 100, 20, callback);

        RecordingActor actor = new RecordingActor();

        persistence.size = 150;
        persistenceBp.enqueue(event(), null);

        assertThrows(UnderPressureException.class, () -> actorBp.dispatch("m", actor));

        persistence.size = 5;
        persistenceBp.enqueue(event(), null);
        actorBp.dispatch("m2", actor);
        assertEquals(1, delegate.dispatched.get());
    }
}
