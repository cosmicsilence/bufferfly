package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.AbstractActor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Microbenchmarks for {@link SpinningActorDispatcher}.
 *
 * <h2>Benchmark 1 — High-Contention MPSC Producer Throughput ("The Firehose")</h2>
 * <p>N virtual-thread producers all race to push messages into the same actor through
 * the MPSC mailbox. Measures end-to-end throughput (messages/sec) from the moment
 * the start gate opens until the actor has consumed every message.
 *
 * <p>Three configurations are compared back-to-back and printed as a table:
 * <ul>
 *   <li>1 producer  × 100 000 messages</li>
 *   <li>10 producers × 10 000 messages each (100 000 total)</li>
 *   <li>50 producers ×  2 000 messages each (100 000 total)</li>
 * </ul>
 *
 * <h2>Benchmark 2 — Burst-to-Idle Latency Profile ("The Ping")</h2>
 * <p>A single producer sends exactly one message and measures the wall-clock time
 * from {@code dispatch()} return until {@code actor.receive()} completes. This is
 * repeated for {@value #PING_ROUNDS} rounds (after a warm-up phase) to build a
 * percentile distribution (p50 / p95 / p99 / max) of the spin-poll latency under
 * idle conditions — i.e., how quickly the busy-spinning thread picks up a lone message
 * when the mailbox was previously empty.
 */
@Timeout(120)
class SpinningActorDispatcherBenchmarkTest {

    // -----------------------------------------------------------------------
    // Benchmark constants
    // -----------------------------------------------------------------------

    /** Total messages dispatched per firehose configuration. */
    private static final int FIREHOSE_TOTAL = 100_000;

    /**
     * Short warmup before the firehose measurements so JIT compilation and
     * AffinityLock acquisition do not skew the first run.
     */
    private static final int FIREHOSE_WARMUP = 2_000;

    /** Number of back-to-back ping rounds measured (after warmup). */
    private static final int PING_ROUNDS = 2_000;

    /** Warmup pings discarded before collecting latency samples. */
    private static final int PING_WARMUP = 200;

    // -----------------------------------------------------------------------
    // Shared fixture
    // -----------------------------------------------------------------------

    private SpinningActorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        // Capacity sized to FIREHOSE_TOTAL so that all 100 000 in-flight messages
        // from up to 50 concurrent producers can be enqueued without any mailbox
        // rejection. The ping benchmark sends one message at a time, so this has
        // no effect there.
        dispatcher = new SpinningActorDispatcher(
                new SpinningActorDispatcher.Config(FIREHOSE_TOTAL));
    }

    @AfterEach
    void tearDown() {
        // Spin threads are stopped inside each benchmark via dispatcher.clear().
    }

    // -----------------------------------------------------------------------
    // Benchmark 1: High-Contention MPSC Producer Throughput
    // -----------------------------------------------------------------------

    /**
     * Sends {@value #FIREHOSE_TOTAL} messages through one actor from 1, 10, and 50
     * concurrent virtual-thread producers and reports msgs/sec for each configuration.
     *
     * <p>Correctness gate: every configuration must deliver 100 % of messages.
     */
    @Test
    @Timeout(120)
    void benchmark_firehose_mpscThroughput_singleActorUnderIncreasingProducerContention()
            throws InterruptedException {

        record Config(int producers, String label) {}

        List<Config> configs = List.of(
                new Config(1,  " 1 producer "),
                new Config(10, "10 producers"),
                new Config(50, "50 producers")
        );

        List<double[]> results = new ArrayList<>(); // [durationMs, throughput]

        for (Config cfg : configs) {
            // --- warmup ---
            runFirehose(cfg.producers(), FIREHOSE_WARMUP, /* printResult= */ false);

            // --- measured run ---
            double[] r = runFirehose(cfg.producers(), FIREHOSE_TOTAL, /* printResult= */ false);
            results.add(r);
        }

        // ===================================================================
        // Print results table
        // ===================================================================
        System.out.println();
        System.out.println("================================================================================");
        System.out.println(" BENCHMARK 1 — HIGH-CONTENTION MPSC PRODUCER THROUGHPUT (\"The Firehose\")");
        System.out.println(" Total messages per run: " + FIREHOSE_TOTAL
                + "  |  Warmup: " + FIREHOSE_WARMUP + " msgs");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" %-14s  %12s  %16s%n", "Configuration", "Duration (ms)", "Throughput (msg/s)");
        System.out.println("--------------------------------------------------------------------------------");

        for (int i = 0; i < configs.size(); i++) {
            double durationMs  = results.get(i)[0];
            double throughput  = results.get(i)[1];
            System.out.printf(" %-14s  %12.2f  %16.0f%n",
                    configs.get(i).label(), durationMs, throughput);
        }

        System.out.println("================================================================================");
        System.out.println();

        // Correctness: throughput must be positive (all messages delivered — enforced
        // inside runFirehose via assertEquals).
        assertTrue(results.stream().allMatch(r -> r[1] > 0));
    }

    /**
     * Runs one firehose configuration.
     *
     * @param producers  number of concurrent virtual-thread producers
     * @param total      total messages to send
     * @param printResult whether to print inline (used for warmup suppression)
     * @return {@code double[]{durationMs, throughputMsgPerSec}}
     */
    private double[] runFirehose(int producers, int total, boolean printResult)
            throws InterruptedException {

        LatchCountingActor actor = new LatchCountingActor("firehose-actor", total);
        actor.start();

        int perProducer = total / producers;
        int remainder   = total % producers;

        CountDownLatch startGate   = new CountDownLatch(1);
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

        // Wait until the actor has consumed all messages
        actor.awaitCompletion(30, TimeUnit.SECONDS);
        long endNs = System.nanoTime();

        assertEquals(total, actor.getReceiveCount(),
                "Firehose must deliver 100% of messages (" + producers + " producers)");

        dispatcher.clear(actor);

        double durationMs  = (endNs - startNs) / 1_000_000.0;
        double throughput  = total / (durationMs / 1_000.0);

        if (printResult) {
            System.out.printf("  producers=%-3d  duration=%.2f ms  throughput=%.0f msg/s%n",
                    producers, durationMs, throughput);
        }

        return new double[]{durationMs, throughput};
    }

    // -----------------------------------------------------------------------
    // Benchmark 2: Burst-to-Idle Latency Profile ("The Ping")
    // -----------------------------------------------------------------------

    /**
     * Sends one message at a time to a spinning actor and records the nanosecond
     * latency from the moment {@code dispatch()} returns until {@code receive()}
     * completes. Builds a p50/p95/p99/max distribution over {@value #PING_ROUNDS}
     * rounds.
     *
     * <p>Correctness gate: all {@value #PING_ROUNDS} messages must be received.
     */
    @Test
    @Timeout(120)
    void benchmark_ping_burstToIdleLatencyProfile_singleMessageRoundTrip()
            throws InterruptedException {

        PingActor actor = new PingActor("ping-actor");
        actor.start();

        long[] latenciesNs = new long[PING_ROUNDS];

        // --- warmup phase ---
        for (int i = 0; i < PING_WARMUP; i++) {
            sendPingAndWait(actor);
        }

        // --- measured phase ---
        // Protocol per round:
        //   1. arm() — install a fresh one-shot latch BEFORE dispatch (eliminates race)
        //   2. capture nanoTime start
        //   3. dispatch the message
        //   4. await() — block until receive() counts the latch down
        //   5. capture nanoTime end → latency = end - start
        for (int round = 0; round < PING_ROUNDS; round++) {
            actor.arm();
            long startNs = System.nanoTime();
            dispatcher.dispatch("ping", actor);
            actor.await();
            long endNs = System.nanoTime();
            latenciesNs[round] = endNs - startNs;
        }

        dispatcher.clear(actor);

        // ===================================================================
        // Percentile computation
        // ===================================================================
        long[] sorted = latenciesNs.clone();
        java.util.Arrays.sort(sorted);

        long p50  = sorted[(int) (PING_ROUNDS * 0.50)];
        long p95  = sorted[(int) (PING_ROUNDS * 0.95)];
        long p99  = sorted[(int) (PING_ROUNDS * 0.99)];
        long max  = sorted[PING_ROUNDS - 1];
        long min  = sorted[0];

        LongSummaryStatistics stats = java.util.Arrays.stream(latenciesNs).summaryStatistics();
        double avgNs = stats.getAverage();

        // ===================================================================
        // Print results
        // ===================================================================
        System.out.println();
        System.out.println("================================================================================");
        System.out.println(" BENCHMARK 2 — BURST-TO-IDLE LATENCY PROFILE (\"The Ping\")");
        System.out.println(" Single message dispatch → receive() latency");
        System.out.println(" Rounds: " + PING_ROUNDS + "  |  Warmup: " + PING_WARMUP + " rounds");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" %-8s  %10s%n", "Percentile", "Latency");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf(" %-8s  %7.2f µs%n", "min",  min  / 1_000.0);
        System.out.printf(" %-8s  %7.2f µs%n", "avg",  avgNs / 1_000.0);
        System.out.printf(" %-8s  %7.2f µs%n", "p50",  p50  / 1_000.0);
        System.out.printf(" %-8s  %7.2f µs%n", "p95",  p95  / 1_000.0);
        System.out.printf(" %-8s  %7.2f µs%n", "p99",  p99  / 1_000.0);
        System.out.printf(" %-8s  %7.2f µs%n", "max",  max  / 1_000.0);
        System.out.println("================================================================================");
        System.out.println();

        // Correctness: all rounds completed
        assertEquals(PING_ROUNDS + PING_WARMUP, actor.getTotalReceived(),
                "Every ping must have been received");

        // Sanity: p99 under 1 ms (the spinning thread never parks; 1 ms would be anomalous)
        assertTrue(p99 < 1_000_000L,
                "p99 latency exceeded 1 ms — check for OS scheduler interference");
    }

    /** Sends one ping and blocks until the actor has processed it (warmup path). */
    private void sendPingAndWait(PingActor actor) throws InterruptedException {
        actor.arm();
        dispatcher.dispatch("ping", actor);
        actor.await();
    }

    // -----------------------------------------------------------------------
    // Actor helpers
    // -----------------------------------------------------------------------

    /**
     * Actor that counts received messages and fires a {@link CountDownLatch}
     * once a target count is reached — used by the firehose benchmark.
     */
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

        @Override public String name() { return actorName; }
        @Override protected void preStart()  {}
        @Override protected void postStop()  {}
        @Override public void onError(String message, Exception e) {}

        int getReceiveCount() { return received.get(); }

        void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
            boolean completed = done.await(timeout, unit);
            if (!completed) {
                throw new AssertionError(
                        "LatchCountingActor timed out — received " + received.get() + " messages");
            }
        }
    }

    /**
     * Actor designed for the ping benchmark.
     *
     * <p>Each call to {@code receive()} counts the message and releases a
     * one-shot latch so the benchmark thread can measure the exact moment
     * processing ended.
     *
     * <p>Race-free protocol per round (single benchmark thread):
     * <pre>{@code
     *   actor.arm();                         // 1. install fresh latch BEFORE dispatch
     *   long t0 = System.nanoTime();
     *   dispatcher.dispatch("ping", actor);  // 2. put message in mailbox
     *   actor.await();                       // 3. block until receive() fires
     *   long latency = System.nanoTime() - t0;
     * }</pre>
     *
     * <p>Because {@code arm()} runs before {@code dispatch()}, the latch is
     * always in place when the spin thread picks up the message. If {@code receive()}
     * somehow fires before {@code await()}, {@code CountDownLatch.await()} returns
     * immediately — no message is ever lost.
     */
    static class PingActor extends AbstractActor<String> {

        private final String actorName;
        private final AtomicInteger totalReceived = new AtomicInteger(0);

        /**
         * One-shot latch armed by the benchmark thread before each dispatch.
         * Declared volatile so the spin thread sees the latest reference.
         */
        private volatile CountDownLatch roundLatch = new CountDownLatch(1);

        PingActor(String name) {
            this.actorName = name;
        }

        @Override
        public void receive(String message) {
            totalReceived.incrementAndGet();
            roundLatch.countDown();   // unblock the awaiting benchmark thread
        }

        @Override public String name() { return actorName; }
        @Override protected void preStart()  {}
        @Override protected void postStop()  {}
        @Override public void onError(String message, Exception e) {}

        /** Step 1 — install a fresh latch before calling {@code dispatch()}. */
        void arm() {
            roundLatch = new CountDownLatch(1);
        }

        /** Step 3 — block until {@code receive()} fires and counts down the latch. */
        void await() throws InterruptedException {
            boolean received = roundLatch.await(5, TimeUnit.SECONDS);
            if (!received) {
                throw new AssertionError("Ping timed out — spin thread may have stalled");
            }
        }

        int getTotalReceived() { return totalReceived.get(); }
    }
}
