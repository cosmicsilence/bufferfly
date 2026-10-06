package io.bufferfly.core.actor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the greedy-actor yield fix in {@link VTDispatcher}.
 *
 * <h2>Why CPU-bound work matters here</h2>
 * A virtual thread doing pure CPU work never unmounts from its carrier — there
 * is no blocking operation to trigger the mount/unmount mechanism. N actors
 * each running a tight CPU loop in their drain loops will pin all N carrier
 * threads with zero scheduling opportunities for other virtual threads.
 *
 * <p>{@code Thread.yield()} every {@code YIELD_THRESHOLD} (32) messages is the
 * only cooperative release valve in this scenario: it re-queues the carrier in
 * the OS run queue, giving the ForkJoinPool a chance to mount the lone actor's
 * pending virtual thread on that carrier before the drain loop continues.
 *
 * <p>A sleep-based test would <em>not</em> prove this — {@code Thread.sleep()}
 * unmounts the carrier on its own, making the yield fix irrelevant to the
 * outcome. Only CPU-bound work that never blocks exposes the real problem.
 *
 * <h2>The test</h2>
 * Spawns {@code N} heavy actors (one per available core) whose {@code receive()}
 * does real CPU work via {@link Math#sqrt} — a native intrinsic the JIT cannot
 * elide, giving ~100 µs of genuine CPU time per message with no blocking.
 * A lone actor receives one message per round (10 rounds total). The lone
 * actor atomically captures how many heavy messages have been processed at the
 * exact moment it is scheduled, proving it ran mid-drain and not after.
 */
@Timeout(120)
class VTDispatcherYieldTest {

    /**
     * {@link Math#sqrt} iterations per message — gives ~100 µs of real CPU
     * work that the JIT cannot elide. At 500 messages per actor each drain
     * loop stays alive for ~50 ms per round.
     */
    private static final int CPU_ITERATIONS = 20_000;

    /**
     * Messages per heavy actor per round.
     */
    private static final int HEAVY_BURST = 500;

    /**
     * Max time the lone actor may wait to receive its message after tell().
     */
    private static final Duration LONE_ACTOR_MAX_WAIT = Duration.ofSeconds(5);

    /**
     * Max time to wait for all heavy actors to drain between rounds.
     */
    private static final Duration HEAVY_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void should_receiveAllLoneMessages_before_heavyActorsFinish() throws InterruptedException {

        final int processors = Runtime.getRuntime().availableProcessors();
        final int ROUNDS = 10;

        ActorManager manager = new ActorManager();

        // ---------------------------------------------------------------
        // processors heavy actors — pure CPU work, never parks, never unmounts
        // ---------------------------------------------------------------
        List<CpuBoundActor> heavyDelegates = new ArrayList<>(processors);
        List<ActorReference<String>> heavyRefs = new ArrayList<>(processors);
        for (int i = 0; i < processors; i++) {
            CpuBoundActor d = new CpuBoundActor("heavy-" + i);
            heavyDelegates.add(d);
            heavyRefs.add(manager.start(d));
        }

        // ---------------------------------------------------------------
        // One lone actor — captures a snapshot of heavy progress the
        // instant it is scheduled, allowing an atomic mid-drain assertion.
        // ---------------------------------------------------------------
        AtomicInteger loneReceived = new AtomicInteger(0);
        AtomicInteger heavyCountAtLoneSchedule = new AtomicInteger(0);

        DummyCountingActor loneDelegate = new DummyCountingActor("lone") {
            @Override
            public void receive(String message) {
                heavyCountAtLoneSchedule.set((int) heavyDelegates.stream().mapToLong(CpuBoundActor::getReceiveCount).sum());
                loneReceived.incrementAndGet();
                super.receive(message);
            }
        };
        ActorReference<String> loneRef = manager.start(loneDelegate);

        // ---------------------------------------------------------------
        // Run ROUNDS iterations
        // ---------------------------------------------------------------
        for (int round = 0; round < ROUNDS; round++) {

            heavyDelegates.forEach(CpuBoundActor::reset);
            heavyCountAtLoneSchedule.set(0);

            // Send the heavy burst — drain loops pin the carriers with CPU
            // work; only Thread.yield() every 32 messages releases a carrier.
            for (ActorReference<String> ref : heavyRefs) {
                for (int m = 0; m < HEAVY_BURST; m++) {
                    ref.tell("msg-" + m);
                }
            }

            // Let drain loops mount on carriers before sending the lone message
            Thread.sleep(10);

            final int expectedLoneCount = round + 1;
            final int currentRound = round;
            loneRef.tell("round-" + round);

            // Lone actor must be scheduled within the deadline
            await().atMost(LONE_ACTOR_MAX_WAIT).untilAsserted(() -> assertEquals(expectedLoneCount, loneReceived.get()
                    , "Round " + currentRound + ": lone actor was not scheduled within " + LONE_ACTOR_MAX_WAIT.toMillis() + " ms "
                            + "— heavy actors are not yielding their carriers"));

            // The snapshot captured atomically inside receive() shows how many
            // heavy messages had been processed when the lone actor was scheduled.
            // It must be less than the total, proving the lone actor ran mid-drain.
            int heavyAtSchedule = heavyCountAtLoneSchedule.get();
            assertTrue(heavyAtSchedule < (long) processors * HEAVY_BURST,
                    "Round " + round + ": snapshot shows heavy actors were already done (" + heavyAtSchedule + "/" + ((long) processors * HEAVY_BURST) + ") " +
                            "when lone actor was scheduled " + "— increase HEAVY_BURST or CPU_ITERATIONS");

            // Wait for heavy actors to finish before next round
            await().atMost(HEAVY_DRAIN_TIMEOUT).untilAsserted(() -> {
                for (CpuBoundActor heavy : heavyDelegates) {
                    assertEquals(HEAVY_BURST, heavy.getReceiveCount(), heavy.name() + " did not finish draining");
                }
            });
        }

        // Correctness gate: lone actor received exactly one message per round
        assertEquals(ROUNDS, loneReceived.get(), "Lone actor must receive exactly one message per round");
    }

    // -----------------------------------------------------------------------
    // CPU-bound actor — Math.sqrt loop, no blocking, no sleep, no I/O
    // -----------------------------------------------------------------------

    /**
     * Actor whose {@code receive()} runs {@value #CPU_ITERATIONS} iterations of
     * {@link Math#sqrt} — a native intrinsic the JIT cannot elide — giving
     * ~100 µs of genuine CPU work per message with zero blocking operations.
     * The virtual thread therefore never unmounts its carrier voluntarily.
     */
    static class CpuBoundActor extends AbstractActor<String> {

        private final String actorName;
        private final AtomicInteger receiveCount = new AtomicInteger(0);

        CpuBoundActor(String name) {
            this.actorName = name;
        }

        @Override
        public void receive(String message) {
            double result = 2.0;
            for (int i = 0; i < CPU_ITERATIONS; i++) {
                result = Math.sqrt(result + i);
            }
            if (result < 0) throw new IllegalStateException("impossible");
            receiveCount.incrementAndGet();
        }

        @Override
        public String name() {
            return actorName;
        }

        @Override
        protected void preStart() {
        }

        @Override
        protected void postStop() {
        }

        @Override
        public void onError(String message, Exception e) {
        }

        int getReceiveCount() {
            return receiveCount.get();
        }

        void reset() {
            receiveCount.set(0);
        }
    }
}
