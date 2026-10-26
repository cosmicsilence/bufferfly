package io.bufferfly.core.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the <b>multiple side effects per event</b> contract: a
 * {@link PersistenceRunner.PersistenceEvent} may carry any number of
 * {@link PersistenceRunner.SideEffect}s, and the runner runs them all, <b>in
 * order</b>, only <b>after</b> the batch holding the event is durably committed.
 */
@Timeout(30)
class MultiSideEffectTest {

    /** A side effect that records its label into a shared ordered list when processed. */
    record Labelled(String label) implements PersistenceRunner.SideEffect { }

    static final class RecordingOps implements TransactionalOperations<String> {
        volatile boolean fail = false;
        @Override
        public long persistOnTransaction(List<String> batch, long currentToken) {
            if (fail) throw new UnrecoverableException("boom");
            return currentToken;
        }
        @Override public long onPoisonPill(String e, long t, Exception c) { return t; }
    }

    private static BatchingPersistenceRunner<String> runner(RecordingOps ops, List<String> processed) {
        PersistenceConfig<String> config = PersistenceConfig.builder(ops)
                .batchSize(10).timeoutMs(10L).maxRetries(0).build();
        // Record each processed side effect's label, preserving invocation order.
        return config.instantiate(se -> processed.add(((Labelled) se).label()));
    }

    @Test
    void should_runAllSideEffects_inOrder_afterCommit() {
        RecordingOps ops = new RecordingOps();
        List<String> processed = new CopyOnWriteArrayList<>();
        BatchingPersistenceRunner<String> runner = runner(ops, processed);

        // One event carrying THREE side effects, plus a second event with one.
        PersistenceRunner.PersistenceEvent<String> e1 =
                PersistenceRunner.newEvent("rec-1",
                        new Labelled("a1"), new Labelled("a2"), new Labelled("a3"));
        PersistenceRunner.PersistenceEvent<String> e2 =
                PersistenceRunner.newEvent("rec-2", new Labelled("b1"));

        assertEquals(3, e1.sideEffects().size(), "the event carries three side effects");

        runner.persistBatch(List.of(e1, e2));

        // All four side effects ran, in event order and within an event in list order.
        assertEquals(List.of("a1", "a2", "a3", "b1"), processed,
                "every side effect of every event must run, in order, after the batch commits");
    }

    @Test
    void should_runNoSideEffects_when_batchFailsToCommit() {
        RecordingOps ops = new RecordingOps();
        ops.fail = true;
        List<String> processed = new CopyOnWriteArrayList<>();
        BatchingPersistenceRunner<String> runner = runner(ops, processed);

        runner.persistBatch(List.of(
                PersistenceRunner.newEvent("rec-1", new Labelled("a1"), new Labelled("a2"))));

        assertTrue(processed.isEmpty(),
                "no side effect may run when the batch never commits (unrecoverable failure)");
    }

    @Test
    void should_supportZeroSideEffects() {
        RecordingOps ops = new RecordingOps();
        List<String> processed = new CopyOnWriteArrayList<>();
        BatchingPersistenceRunner<String> runner = runner(ops, processed);

        PersistenceRunner.PersistenceEvent<String> none = PersistenceRunner.newEvent("rec-1");
        assertTrue(none.sideEffects().isEmpty(), "newEvent(e) carries no side effects");

        runner.persistBatch(List.of(none));
        assertTrue(processed.isEmpty(), "an event with no side effects runs none");
    }

    @Test
    void should_acceptSideEffectList_factory() {
        PersistenceRunner.PersistenceEvent<String> e = PersistenceRunner.newEvent(
                "rec", List.of(new Labelled("x"), new Labelled("y")));
        assertEquals(2, e.sideEffects().size());
        // the list is held immutably
        assertEquals("x", ((Labelled) e.sideEffects().get(0)).label());
    }
}
