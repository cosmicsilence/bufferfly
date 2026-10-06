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
        public void run(List<String> batch) {
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
        var config = new BatchingPersistenceRunner.Config(batchSize, timeoutMs, 0);
        return new BatchingPersistenceRunner<>(spy, config, () -> {
        }, dispatcher);
    }


    @Test
    void shouldRespectBatchSizes() {

        final int TOTAL_ELEMENTS = 10000;
        final int BATCH_SIZE = 500;
        SpyOperations spy = new SpyOperations();
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(BATCH_SIZE, 50);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, BATCH_SIZE, 50);

        for (int t = 0; t < TOTAL_ELEMENTS; t++) {
            runner.enqueue("e-" + t);
        }

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(spy.flushes, everyItem(lessThanOrEqualTo(BATCH_SIZE))));
    }


// -----------------------------------------------------------------------
// Test 2 — single-writer invariant under sustained concurrent load
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
                for (int i = 0; i < EVENTS_EACH; i++) runner.enqueue("e-" + (base + i));
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
