package io.bufferfly.core.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for  {@link NaturalBatchingDispatcher}.
 */
@Timeout(60)
public class NaturalBatchingDispatcherTest {


    // -----------------------------------------------------------------------
    // Spy operations
    // -----------------------------------------------------------------------

    static class SpyOperations implements TransactionalOperations<String> {
        final AtomicInteger flushed = new AtomicInteger(0);
        final AtomicInteger concurrent = new AtomicInteger(0);
        final AtomicInteger maxConcurrent = new AtomicInteger(0);
        final List<Integer> flushes = new CopyOnWriteArrayList<>();

        @Override
        public void persistOnTransaction(List<String> batch) {
            int depth = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(depth, Math::max);
            flushed.addAndGet(batch.size());
            concurrent.decrementAndGet();
            flushes.add(batch.size());
        }

        @Override
        public void onPoisonPill(String event, Exception cause) {
        }
    }

    static BatchingPersistenceRunner<String> runner(SpyOperations spy,
                                                    NaturalBatchingDispatcher<String> dispatcher,
                                                    int batchSize, long timeoutMs) {
        var config = new PersistenceConfig<>(spy, batchSize, timeoutMs, 0, () -> {}, dispatcher);
        return config.instantiate();
    }


    @Test
    void shouldRespectBatchSizes() throws InterruptedException {

        final int PRODUCERS = 10;
        final int EVENTS_EACH = 1_000;
        final int TOTAL_ELEMENTS = PRODUCERS * EVENTS_EACH; // 10_000
        final int BATCH_SIZE = 50;

        SpyOperations spy = new SpyOperations();
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(BATCH_SIZE, 50);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, BATCH_SIZE, 50);

        // Realistic load: 10 virtual-thread producers hammer the buffer
        // concurrently (not a single serial loop), all released together so
        // their enqueues actually interleave.
        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(PRODUCERS);

        for (int p = 0; p < PRODUCERS; p++) {
            final int base = p * EVENTS_EACH;
            Thread.ofVirtual().start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < EVENTS_EACH; i++) {
                    runner.enqueue(PersistenceRunner.newEvent("e-" + (base + i)));
                }
                done.countDown();
            });
        }

        gate.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS), "producers did not finish enqueuing in time");

        // Every flush must honour the batch-size cap, and all events must land.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    assertEquals(TOTAL_ELEMENTS, spy.flushed.get(),
                            "all enqueued events must be flushed");
                    assertThat(spy.flushes, everyItem(lessThanOrEqualTo(BATCH_SIZE)));
                });
    }


// -----------------------------------------------------------------------
// Test 2 — natural-batching timeout: a slow trickle must be coalesced
//          within the timeoutMs window (light-load path)
// -----------------------------------------------------------------------

    /**
     * Natural batching must <em>wait</em> up to {@code timeoutMs} collecting
     * whatever arrives in that window into a single batch — not return after the
     * first couple of polls. Under a slow trickle (events spaced a few ms apart,
     * never filling {@code batchSize}), the drain loop should therefore gather
     * several trickled events per batch.
     *
     * <p>This exercises the light-load / timeout path that the saturated
     * {@link #shouldRespectBatchSizes()} test cannot: a drain loop that fails to
     * honour the timeout window would flush each trickled event in its own tiny
     * batch, so no batch would ever coalesce many of them.
     */
    @Test
    void should_coalesceTrickleWithinTimeoutWindow_when_eventsArriveSlowly() throws InterruptedException {
        final int EVENTS = 60;
        final long GAP_MS = 3;      // well inside the timeout window
        final int BATCH_SIZE = 500; // large, so batchSize never bounds a trickle
        final long TIMEOUT_MS = 50; // the max allowed window

        SpyOperations spy = new SpyOperations();
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(BATCH_SIZE, TIMEOUT_MS);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, BATCH_SIZE, TIMEOUT_MS);

        // A single slow producer: one event every GAP_MS. At ~3ms spacing, a
        // correct 50ms window should sweep up roughly a dozen events per batch.
        for (int i = 0; i < EVENTS; i++) {
            runner.enqueue(PersistenceRunner.newEvent("e-" + i));
            Thread.sleep(GAP_MS);
        }

        // All events must eventually land...
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(EVENTS, spy.flushed.get(),
                        "all trickled events must be flushed"));

        // ...and natural batching must have coalesced the trickle: at least one
        // batch holds several events (far more than the ~1-2 a non-waiting drain
        // loop could ever gather under this spacing).
        int largestBatch = spy.flushes.stream().mapToInt(Integer::intValue).max().orElse(0);
        assertTrue(largestBatch >= 5,
                "timeout window must coalesce a trickle into a batch of several events, but the largest batch was "
                        + largestBatch + " (flushes=" + spy.flushes + ")");
    }

// -----------------------------------------------------------------------
// Test 3 — single-writer invariant under sustained concurrent load
// -----------------------------------------------------------------------

    @Test
    void should_neverCallRunConcurrently_when_manyProducersEnqueueSimultaneously()
            throws InterruptedException {

        final int THREADS = 30;
        final int EVENTS_EACH = 100;
        final int TOTAL = THREADS * EVENTS_EACH;

        SpyOperations spy = new SpyOperations();
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(20, 5);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, 20, 5);

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int t = 0; t < THREADS; t++) {
            final int base = t * EVENTS_EACH;
            Thread.ofVirtual().start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < EVENTS_EACH; i++) runner.enqueue(PersistenceRunner.newEvent("e-" + (base + i)));
                done.countDown();
            });
        }

        gate.countDown();
        done.await(20, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(TOTAL, spy.flushed.get()));

        assertEquals(1, spy.maxConcurrent.get(),
                "run() must never be called concurrently — single-writer violation");
    }
}
