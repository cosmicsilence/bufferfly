package io.bufferfly.core.persistence;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the classification of a {@link TransientException} thrown
 * <em>during item-by-item isolation</em> (after a batch raised a
 * {@link SingleItemException}).
 *
 * <p>Because {@link TransientException} {@code extends} {@link PersistenceException},
 * a naive {@code catch (PersistenceException)} in the isolation path would
 * mis-classify a transient failure on a <b>good</b> record as a poison pill and
 * dead-letter it — then advance the fence past it, so a restart's rewind would
 * <b>skip that good record entirely</b> (silent data loss).
 *
 * <p>The correct behavior: a transient failure mid-isolation is NOT a poison
 * pill. The runner must halt (invoking the unrecoverable/restart hook) and leave
 * the good record un-dead-lettered and its fence un-advanced, so the restart
 * replays it from the committed offset.
 */
class TransientDuringIsolationTest {

    /** A token-bearing event; {@code poison} forces the store to reject it as a pill. */
    record Rec(long token, boolean poison) {
    }

    /**
     * Models the JDBC fencing store (same shape as {@code PoisonPillTokenAdvanceTest})
     * but additionally throws a one-shot {@link TransientException} the first time
     * a specific token is persisted as a lone item during isolation.
     */
    static final class FlakyFencingOps implements TransactionalOperations<Rec> {
        long storedToken;
        final List<Long> persisted = new ArrayList<>();
        final List<Long> deadLettered = new ArrayList<>();

        /** The token whose FIRST lone-item persist throws a transient error. */
        private final long transientOnceForToken;
        private boolean transientFired = false;

        FlakyFencingOps(long storedToken, long transientOnceForToken) {
            this.storedToken = storedToken;
            this.transientOnceForToken = transientOnceForToken;
        }

        private long fenceAndAdvance(long currentToken, long newMax) {
            if (currentToken < 0) {
                storedToken = newMax;
                return storedToken;
            }
            if (newMax <= currentToken) {
                return storedToken;
            }
            if (currentToken != storedToken) {
                throw new UnrecoverableException(
                        "fence failed: expected storedToken=" + storedToken
                                + " but caller held " + currentToken);
            }
            storedToken = newMax;
            return storedToken;
        }

        @Override
        public long persistOnTransaction(List<Rec> batch, long currentToken) {
            if (batch == null || batch.isEmpty()) return currentToken;

            // A multi-item batch containing a poison pill is rejected wholesale so
            // the runner falls back to item-by-item isolation (as a DB constraint would).
            if (batch.size() > 1 && batch.stream().anyMatch(Rec::poison)) {
                throw new SingleItemException("batch contains a poison pill");
            }

            Rec single = batch.get(0);

            // One-shot transient failure for a specific (good) token, triggered on
            // its lone-item persist during isolation. This models a momentary DB
            // deadlock / connection blip on a record that would otherwise succeed.
            if (!single.poison() && single.token() == transientOnceForToken && !transientFired) {
                transientFired = true;
                throw new TransientException("momentary DB blip persisting token " + single.token());
            }

            if (single.poison()) {
                throw new SingleItemException("poison pill at token " + single.token());
            }

            long max = batch.stream().mapToLong(Rec::token).max().orElseThrow();
            long advanced = fenceAndAdvance(currentToken, max);
            persisted.addAll(batch.stream().map(Rec::token).toList());
            return advanced;
        }

        @Override
        public long onPoisonPill(Rec event, long currentToken, Exception cause) {
            deadLettered.add(event.token());
            return fenceAndAdvance(currentToken, event.token());
        }
    }

    @Test
    void should_notDeadLetterGoodRecord_when_itThrowsTransientDuringIsolation() {
        // token 1 (good) will throw a one-shot TransientException during isolation.
        FlakyFencingOps ops = new FlakyFencingOps(0L, /* transientOnceForToken */ 1L);
        boolean[] unrecoverable = {false};
        // maxRetries irrelevant to the bug; isolation has no retry loop.
        PersistenceConfig<Rec> config = new PersistenceConfig<>(ops, 10, 3, () -> unrecoverable[0] = true);
        BatchingPersistenceRunner<Rec> runner = config.instantiate();
        runner.initToken(0L);

        // Batch: good(1), POISON(2), good(3). The batch is rejected wholesale
        // (contains a pill) -> isolation. During isolation, good(1) hits a
        // TransientException. It must NOT be treated as a poison pill.
        List<PersistenceRunner.PersistenceEvent<Rec>> batch = List.of(
                PersistenceRunner.newEvent(new Rec(1, false)),
                PersistenceRunner.newEvent(new Rec(2, true)),
                PersistenceRunner.newEvent(new Rec(3, false)));

        runner.persistBatch(batch);

        // CORRECT behavior: the transient failure on good(1) halts for replay.
        // good(1) must NOT be dead-lettered, and the fence must NOT advance past it.
        assertFalse(ops.deadLettered.contains(1L),
                "a good record that hit a TransientException must NOT be dead-lettered as a poison pill");
        assertEquals(List.of(), ops.deadLettered,
                "nothing should be dead-lettered: isolation halted on the transient error before reaching the real pill");
        assertEquals(0L, ops.storedToken,
                "the fence must NOT advance past a record that failed transiently (it will be replayed on restart)");
        assertEquals(0L, runner.currentToken(),
                "the runner must still hold the pre-batch token so the restart replays from a known-good position");
        assertTrue(unrecoverable[0],
                "a transient failure mid-isolation must halt (invoke the unrecoverable/restart hook) for replay");
    }
}
