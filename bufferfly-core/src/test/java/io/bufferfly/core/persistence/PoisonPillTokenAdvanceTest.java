package io.bufferfly.core.persistence;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the fencing-token contract when a poison pill is isolated
 * <em>in the middle</em> of a batch, under the <b>halt-at-pill</b> policy:
 *
 * <ul>
 *   <li>good records <em>before</em> the pill persist and advance the fence;</li>
 *   <li>the pill is dead-lettered via {@link TransactionalOperations#onPoisonPill},
 *       which advances the stored token <em>past</em> it so a restart's rewind
 *       resumes AFTER the pill rather than re-delivering it;</li>
 *   <li>the runner then <b>halts</b> — good records <em>after</em> the pill in the
 *       same batch are deliberately NOT persisted. They are re-processed when the
 *       actor restarts and replays from the committed offset, so the store never
 *       gets ahead of the rejected record.</li>
 * </ul>
 *
 * <p>The crucial token invariant is still asserted: after dead-lettering, the
 * runner's in-memory {@code currentToken} must equal the fence advanced past the
 * pill (not a stale pre-pill value), so that whatever it does next fences from
 * the right place.
 *
 * <p>The fake operations below is a faithful model of the real JDBC fence: it
 * holds one {@code storedToken} and only accepts a write whose
 * {@code currentToken} equals it (a CAS). A stale token therefore surfaces as
 * an {@link UnrecoverableException}, exactly as a real zombie fence would.
 */
class PoisonPillTokenAdvanceTest {

    /** A token-bearing event; {@code poison} forces the store to reject it. */
    record Rec(long token, boolean poison) {
    }

    /**
     * Models the JDBC fencing store: a single authoritative {@code storedToken}
     * advanced only by a compare-and-set against the {@code currentToken} the
     * caller claims to hold. Mirrors {@code FencingOffsetRepository.fenceAndAdvance}
     * and {@code AbstractJdbcTransactionalOperations}.
     */
    static final class FakeFencingOps implements TransactionalOperations<Rec> {
        long storedToken;                        // the authoritative committed token
        final List<Long> persisted = new ArrayList<>();
        final List<Long> deadLettered = new ArrayList<>();

        FakeFencingOps(long storedToken) {
            this.storedToken = storedToken;
        }

        private long fenceAndAdvance(long currentToken, long newMax) {
            if (currentToken < 0) {              // first write ever → insert
                storedToken = newMax;
                return storedToken;
            }
            if (newMax <= currentToken) {        // benign replay, no-op
                return storedToken;
            }
            if (currentToken != storedToken) {   // CAS miss → zombie / stale token
                throw new UnrecoverableException(
                        "fence failed: expected storedToken=" + storedToken
                                + " but caller held " + currentToken);
            }
            storedToken = newMax;                // CAS hit → advance
            return storedToken;
        }

        @Override
        public long persistOnTransaction(List<Rec> batch, long currentToken) {
            if (batch == null || batch.isEmpty()) return currentToken;
            // A batch containing a poison pill is rejected wholesale so the runner
            // falls back to item-by-item isolation (as a real DB constraint would).
            if (batch.size() > 1 && batch.stream().anyMatch(Rec::poison)) {
                throw new SingleItemException("batch contains a poison pill");
            }
            Rec single = batch.get(0);
            if (single.poison()) {
                // A lone poison pill: the fence is NOT advanced (the write is
                // rejected), and the runner is told to isolate/dead-letter it.
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
            // Step the fence past the dead-lettered token, in the same spirit as
            // the real base class, and hand the advanced token back to the runner.
            return fenceAndAdvance(currentToken, event.token());
        }
    }

    @Test
    void should_persistPrePillAndFencePastPill_but_abandonPostPill_when_poisonPillIsInTheMiddleOfABatch() {
        FakeFencingOps ops = new FakeFencingOps(0L);
        boolean[] unrecoverable = {false};
        PersistenceConfig<Rec> config = new PersistenceConfig<>(ops, 10, 10L, 0, () -> unrecoverable[0] = true);
        BatchingPersistenceRunner<Rec> runner = config.instantiate();

        // Restart at a known-good token: token 0 already committed.
        runner.initToken(0L);

        // Batch: good(1), POISON(2), good(3). The pill is isolated: the fence
        // advances 0→1 (good), then 1→2 via onPoisonPill (pill dead-lettered),
        // and the runner HALTS — good(3) is abandoned, to be replayed on restart.
        List<PersistenceRunner.PersistenceEvent<Rec>> batch = List.of(
                PersistenceRunner.newEvent(new Rec(1, false)),
                PersistenceRunner.newEvent(new Rec(2, true)),
                PersistenceRunner.newEvent(new Rec(3, false)));

        runner.persistBatch(batch);

        // Only the record BEFORE the pill is persisted; the trailing good record
        // (token 3) is deliberately NOT persisted — it is re-processed on restart.
        assertEquals(List.of(1L), ops.persisted,
                "only the good record before the pill must be persisted; the one after it is abandoned");
        assertEquals(List.of(2L), ops.deadLettered, "the poison pill must be dead-lettered");
        assertEquals(2L, ops.storedToken, "the fence must have advanced past the pill (to token 2)");
        assertEquals(2L, runner.currentToken(),
                "the runner must hold the fence advanced past the pill (2), never a stale pre-pill value");
        assertTrue(unrecoverable[0], "halting at the pill must invoke the unrecoverable/restart hook");
    }
}
