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
        public long onPoisonPill(String event, long currentToken, Exception cause) {
            return currentToken;
        }
    }

    static BatchingPersistenceRunner<String> runner(TransactionalOperations<String> ops,
                                                    NaturalBatchingDispatcher<String> dispatcher,
                                                    int batchSize) {
        var config = new PersistenceConfig<>(ops, batchSize, 0, () -> {}, dispatcher);
        return config.instantiate();
    }


    @Test
    void shouldRespectBatchSizes() throws InterruptedException {

        final int PRODUCERS = 10;
        final int EVENTS_EACH = 1_000;
        final int TOTAL_ELEMENTS = PRODUCERS * EVENTS_EACH; // 10_000
        final int BATCH_SIZE = 50;

        SpyOperations spy = new SpyOperations();
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(BATCH_SIZE);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, BATCH_SIZE);

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
// Test 2 — pure natural batching: never wait; batch what accumulated while
//          the previous batch was being written
// -----------------------------------------------------------------------

    /** Operations whose first write blocks until released, to hold a batch "in flight". */
    static final class GatedOperations implements TransactionalOperations<String> {
        final CountDownLatch firstWriteStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstWrite = new CountDownLatch(1);
        final List<List<String>> batches = new CopyOnWriteArrayList<>();

        @Override
        public void persistOnTransaction(List<String> batch) {
            batches.add(List.copyOf(batch));
            if (batches.size() == 1) {
                firstWriteStarted.countDown();
                try {
                    releaseFirstWrite.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public long onPoisonPill(String event, long currentToken, Exception cause) {
            return currentToken;
        }
    }

    /**
     * Natural batching never waits for more events: a lone event is written
     * immediately, and the events that arrive while that write is in flight form
     * the next batch together.
     */
    @Test
    void should_persistImmediately_andBatchWhatArrivesDuringAWrite() throws InterruptedException {
        GatedOperations ops = new GatedOperations();
        BatchingPersistenceRunner<String> runner = runner(ops, new NaturalBatchingDispatcher<>(500), 500);

        // A lone event is written on its own, without waiting for company.
        runner.enqueue(PersistenceRunner.newEvent("first"));
        assertTrue(ops.firstWriteStarted.await(5, TimeUnit.SECONDS), "a lone event must be persisted immediately");
        assertEquals(List.of(List.of("first")), ops.batches, "the first batch holds only the lone event");

        // While that write is in flight, more events accumulate in the buffer...
        for (int i = 0; i < 10; i++) {
            runner.enqueue(PersistenceRunner.newEvent("e-" + i));
        }
        ops.releaseFirstWrite.countDown();

        // ...and are written together as the next natural batch.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(2, ops.batches.size()));
        assertEquals(10, ops.batches.get(1).size(), "events buffered during a write form one batch");
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
        NaturalBatchingDispatcher<String> dispatcher = new NaturalBatchingDispatcher<>(20);
        BatchingPersistenceRunner<String> runner = runner(spy, dispatcher, 20);

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
