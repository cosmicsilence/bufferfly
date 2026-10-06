package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.AbstractActor;
import io.bufferfly.core.actor.VTDispatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LongSummaryStatistics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Head-to-head microbenchmark: {@link SpinningActorDispatcher} vs {@link VTDispatcher}.
 *
 * <p>Both dispatchers expose the same {@link io.bufferfly.core.actor.Dispatcher} contract
 * but make opposite runtime tradeoffs:
 *
 * <ul>
 *   <li><b>SpinningActorDispatcher</b> — one dedicated OS platform thread per actor that
 *       busy-spins forever on a lock-free {@link MpscMailbox} (JCTools MPSC queue).
 *       Zero park/unpark overhead; costs one physical core at 100% utilization always.</li>
 *   <li><b>VTDispatcher</b> — spawns a fresh virtual thread per drain cycle on a
 *       {@link io.bufferfly.core.actor.BlockingMailbox} ({@link java.util.concurrent.LinkedBlockingQueue}).
 *       Near-zero idle cost; pays virtual-thread scheduling overhead on each wake-up.</li>
 * </ul>
 *
 * <h2>Scenario 1 — High-Throughput Firehose</h2>
 * <p>N virtual-thread producers saturate one actor with a large burst of messages.
 * Measures end-to-end throughput (msg/s) for both dispatchers across three producer
 * counts (1, 10, 50) so the MPSC contention slope is visible.
 *
 * <h2>Scenario 2 — Burst-to-Idle Ping Latency</h2>
 * <p>One message sent to an idle actor, repeated for many rounds. Compares the
 * p50/p95/p99/max latency distributions — the regime where virtual-thread
 * scheduling overhead is most visible relative to the spinning poll interval.
 *
 * <p><b>Correctness gate:</b> every scenario asserts that 100% of messages were
 * delivered, so a passing build guarantees the numbers are not inflated by drops.
 */
@Timeout(300)
class DispatcherComparisonBenchmarkTest {

    // -----------------------------------------------------------------------
    // Scenario constants
    // -----------------------------------------------------------------------

    private static final int FIREHOSE_TOTAL  = 100_000;
    private static final int FIREHOSE_WARMUP = 2_000;

    private static final int PING_ROUNDS = 2_000;
    private static final int PING_WARMUP = 200;

    // -----------------------------------------------------------------------
    // Scenario 1 — High-Throughput Firehose
    // -----------------------------------------------------------------------

    @Test
    @Timeout(300)
    void benchmark_firehose_spinningVsVT_singleActorUnderIncreasingProducerContention()
            throws InterruptedException {

        int[] producerCounts = {1, 10, 50};

        double[][] spinResults = new double[producerCounts.length][2]; // [durationMs, throughput]
        double[][] vtResults   = new double[producerCounts.length][2];

        for (int i = 0; i < producerCounts.length; i++) {
            int producers = producerCounts[i];

            // Spinning: mailbox sized to hold the full burst so no offer is rejected.
            SpinningActorDispatcher spinDispatcher =
                    new SpinningActorDispatcher(new SpinningActorDispatcher.Config(FIREHOSE_TOTAL));
            VTDispatcher vtDispatcher = new VTDispatcher(FIREHOSE_TOTAL);

            // warmup
            runFirehose(spinDispatcher, producers, FIREHOSE_WARMUP);
            runFirehose(vtDispatcher,   producers, FIREHOSE_WARMUP);

            // measured
            spinResults[i] = runFirehose(spinDispatcher, producers, FIREHOSE_TOTAL);
            vtResults[i]   = runFirehose(vtDispatcher,   producers, FIREHOSE_TOTAL);
        }

        // ===================================================================
        // Print results
        // ===================================================================
        System.out.println();
        System.out.println("================================================================================");
        System.out.println(" BENCHMARK — FIREHOSE THROUGHPUT COMPARISON");
        System.out.printf(" Total messages: %,d  |  Warmup: %,d%n", FIREHOSE_TOTAL, FIREHOSE_WARMUP);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" %-14s  %20s  %20s  %10s%n",
                "Producers", "Spinning (msg/s)", "VTDispatcher (msg/s)", "Speedup");
        System.out.println("--------------------------------------------------------------------------------");

        for (int i = 0; i < producerCounts.length; i++) {
            double spinTp = spinResults[i][1];
            double vtTp   = vtResults[i][1];
            double speedup = spinTp / vtTp;
            System.out.printf(" %-14s  %20.0f  %20.0f  %9.2fx%n",
                    producerCounts[i] + " producer" + (producerCounts[i] > 1 ? "s" : " "),
                    spinTp, vtTp, speedup);
        }

        System.out.println("================================================================================");
        System.out.println();
    }

    /**
     * Runs one firehose scenario for the given dispatcher.
     *
     * @return {@code double[]{durationMs, throughputMsgPerSec}}
     */
    private double[] runFirehose(io.bufferfly.core.actor.Dispatcher dispatcher,
                                  int producers, int total)
            throws InterruptedException {

        LatchCountingActor actor = new LatchCountingActor("firehose-cmp-actor", total);
        actor.start();

        int perProducer = total / producers;
        int remainder   = total % producers;

        CountDownLatch startGate    = new CountDownLatch(1);
        CountDownLatch producersDone = new CountDownLatch(producers);

        for (int p = 0; p < producers; p++) {
            final int count = perProducer + (p < remainder ? 1 : 0);
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < count; i++) {
                    dispatcher.dispatch("msg", actor);
                }
                producersDone.countDown();
            });
        }

        long startNs = System.nanoTime();
        startGate.countDown();
        producersDone.await(30, TimeUnit.SECONDS);

        actor.awaitCompletion(30, TimeUnit.SECONDS);
        long endNs = System.nanoTime();

        assertEquals(total, actor.getReceiveCount(),
                "100% delivery required for a valid benchmark result");

        if (dispatcher instanceof SpinningActorDispatcher sd) sd.clear(actor);
        if (dispatcher instanceof VTDispatcher vt)          vt.clear(actor);

        double durationMs = (endNs - startNs) / 1_000_000.0;
        double throughput = total / (durationMs / 1_000.0);
        return new double[]{durationMs, throughput};
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Burst-to-Idle Ping Latency
    // -----------------------------------------------------------------------

    @Test
    @Timeout(120)
    void benchmark_ping_spinningVsVT_burstToIdleLatencyComparison()
            throws InterruptedException {

        SpinningActorDispatcher spinDispatcher =
                new SpinningActorDispatcher(new SpinningActorDispatcher.Config(PING_ROUNDS + PING_WARMUP));
        VTDispatcher vtDispatcher = new VTDispatcher(PING_ROUNDS + PING_WARMUP);

        long[] spinLatencies = runPing(spinDispatcher);
        long[] vtLatencies   = runPing(vtDispatcher);

        // ===================================================================
        // Print results
        // ===================================================================
        System.out.println();
        System.out.println("================================================================================");
        System.out.println(" BENCHMARK — BURST-TO-IDLE PING LATENCY COMPARISON");
        System.out.printf(" Rounds: %,d  |  Warmup: %,d%n", PING_ROUNDS, PING_WARMUP);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" %-10s  %18s  %18s  %12s%n",
                "Percentile", "Spinning (µs)", "VTDispatcher (µs)", "Ratio (VT/Spin)");
        System.out.println("--------------------------------------------------------------------------------");

        printPingRow("min",  percentile(spinLatencies, 0.00), percentile(vtLatencies, 0.00));
        printPingRow("avg",  avg(spinLatencies),               avg(vtLatencies));
        printPingRow("p50",  percentile(spinLatencies, 0.50), percentile(vtLatencies, 0.50));
        printPingRow("p95",  percentile(spinLatencies, 0.95), percentile(vtLatencies, 0.95));
        printPingRow("p99",  percentile(spinLatencies, 0.99), percentile(vtLatencies, 0.99));
        printPingRow("max",  percentile(spinLatencies, 1.00), percentile(vtLatencies, 1.00));

        System.out.println("================================================================================");
        System.out.println();
    }

    /**
     * Runs the ping benchmark for the given dispatcher.
     * Returns an array of {@value #PING_ROUNDS} nanosecond latency samples.
     */
    private long[] runPing(io.bufferfly.core.actor.Dispatcher dispatcher)
            throws InterruptedException {

        PingActor actor = new PingActor("ping-cmp-actor");
        actor.start();

        // warmup
        for (int i = 0; i < PING_WARMUP; i++) {
            actor.arm();
            dispatcher.dispatch("ping", actor);
            actor.await();
        }

        // measured
        long[] latencies = new long[PING_ROUNDS];
        for (int round = 0; round < PING_ROUNDS; round++) {
            actor.arm();
            long t0 = System.nanoTime();
            dispatcher.dispatch("ping", actor);
            actor.await();
            latencies[round] = System.nanoTime() - t0;
        }

        if (dispatcher instanceof SpinningActorDispatcher sd) sd.clear(actor);
        if (dispatcher instanceof VTDispatcher vt)          vt.clear(actor);

        return latencies;
    }

    // -----------------------------------------------------------------------
    // Print helpers
    // -----------------------------------------------------------------------

    private void printPingRow(String label, double spinNs, double vtNs) {
        double ratio = vtNs / spinNs;
        System.out.printf(" %-10s  %18.2f  %18.2f  %11.2fx%n",
                label, spinNs / 1_000.0, vtNs / 1_000.0, ratio);
    }

    private double percentile(long[] sorted, double p) {
        long[] s = sorted.clone();
        java.util.Arrays.sort(s);
        if (p >= 1.0) return s[s.length - 1];
        return s[(int) (s.length * p)];
    }

    private double avg(long[] values) {
        LongSummaryStatistics stats = java.util.Arrays.stream(values).summaryStatistics();
        return stats.getAverage();
    }

    // -----------------------------------------------------------------------
    // Actor helpers
    // -----------------------------------------------------------------------

    static class LatchCountingActor extends AbstractActor<String> {

        private final String actorName;
        private final AtomicInteger received = new AtomicInteger(0);
        private final CountDownLatch done;

        LatchCountingActor(String name, int target) {
            this.actorName = name;
            this.done = new CountDownLatch(target);
        }

        @Override
        public void receive(String message) {
            received.incrementAndGet();
            done.countDown();
        }

        @Override public String name()           { return actorName; }
        @Override protected void preStart()      {}
        @Override protected void postStop()      {}
        @Override public void onError(String m, Exception e) {}

        int getReceiveCount() { return received.get(); }

        void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
            if (!done.await(timeout, unit)) {
                throw new AssertionError(
                        "Timed out waiting for all messages — received " + received.get());
            }
        }
    }

    /**
     * Single-message ping actor. Protocol per round:
     * <ol>
     *   <li>{@link #arm()} — install fresh latch <em>before</em> dispatch</li>
     *   <li>dispatch the message</li>
     *   <li>{@link #await()} — block until {@code receive()} fires</li>
     * </ol>
     */
    static class PingActor extends AbstractActor<String> {

        private final String actorName;
        private final AtomicInteger totalReceived = new AtomicInteger(0);
        private volatile CountDownLatch roundLatch = new CountDownLatch(1);

        PingActor(String name) { this.actorName = name; }

        @Override
        public void receive(String message) {
            totalReceived.incrementAndGet();
            roundLatch.countDown();
        }

        @Override public String name()           { return actorName; }
        @Override protected void preStart()      {}
        @Override protected void postStop()      {}
        @Override public void onError(String m, Exception e) {}

        void arm() { roundLatch = new CountDownLatch(1); }

        void await() throws InterruptedException {
            if (!roundLatch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Ping timed out after 5 s");
            }
        }
    }
}
