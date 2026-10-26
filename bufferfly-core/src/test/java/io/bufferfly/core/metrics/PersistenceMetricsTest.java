package io.bufferfly.core.metrics;

import io.bufferfly.core.backpressure.BackpressureCallback;
import io.bufferfly.core.backpressure.BackpressurePersistenceDispatcher;
import io.bufferfly.core.persistence.BatchingPersistenceRunner;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceDispatcher;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.core.persistence.SingleItemException;
import io.bufferfly.core.persistence.TransactionalOperations;
import io.bufferfly.core.persistence.TransientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the persistence-layer and backpressure collectors fire on the real
 * code paths — and are <b>labelled by the owning component (actor) name</b> —
 * when an {@link InMemoryBufferflyMetrics} is wired through
 * {@link PersistenceConfig}:
 * <ul>
 *   <li>a committed batch increments the batch-persisted counter for that
 *       component (and adds the event count);</li>
 *   <li>a poison pill increments the poison-pill counter and the unrecoverable
 *       counter for that component (the runner halts);</li>
 *   <li>exhausted transient retries increment the retry counter and the
 *       unrecoverable counter;</li>
 *   <li>the persistence buffer-depth gauge is registered under the component name;</li>
 *   <li>backpressure engage/release edges increment their component counters.</li>
 * </ul>
 */
@Timeout(30)
class PersistenceMetricsTest {

    private static final String ACTOR = "orders-actor-7";

    record Item(int id, boolean poison, boolean transientFail) { }

    /** A fake store driven purely by the item flags, modelling the runner's branches. */
    static final class FakeOps implements TransactionalOperations<Item> {
        final AtomicInteger deadLettered = new AtomicInteger();

        @Override
        public long persistOnTransaction(List<Item> batch, long currentToken) {
            if (batch.size() > 1 && batch.stream().anyMatch(Item::poison)) {
                throw new SingleItemException("batch contains a poison pill");
            }
            Item only = batch.get(0);
            if (only.poison()) throw new SingleItemException("lone poison pill " + only.id());
            if (only.transientFail()) throw new TransientException("transient " + only.id());
            return currentToken;
        }

        @Override
        public long onPoisonPill(Item event, long currentToken, Exception cause) {
            deadLettered.incrementAndGet();
            return currentToken;
        }
    }

    private static PersistenceConfig<Item> config(InMemoryBufferflyMetrics metrics, FakeOps ops, int maxRetries) {
        return PersistenceConfig.builder(ops)
                .batchSize(10).timeoutMs(10L).maxRetries(maxRetries)
                .metrics(metrics).componentName(ACTOR).build();
    }

    @Test
    void should_countBatchPersisted_and_registerBufferGauge_underComponentName() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        BatchingPersistenceRunner<Item> runner = config(metrics, new FakeOps(), 0).instantiate();

        // The default NaturalBatchingDispatcher registers the buffer-depth gauge
        // keyed by the component (actor) name, not a generic label.
        assertTrue(metrics.hasPersistenceBufferDepthGauge(ACTOR),
                "a persistence buffer-depth gauge must be registered under the actor name");
        assertEquals(0, metrics.persistenceBufferDepth(ACTOR), "an idle buffer reports depth 0");

        runner.persistBatch(List.of(
                PersistenceRunner.newEvent(new Item(1, false, false)),
                PersistenceRunner.newEvent(new Item(2, false, false)),
                PersistenceRunner.newEvent(new Item(3, false, false))));

        assertEquals(1, metrics.batchesPersistedFor(ACTOR), "one committed batch for this actor");
        assertEquals(3, metrics.eventsPersistedFor(ACTOR), "three events in that batch");
        assertEquals(3, metrics.eventsPersistedTotal(), "aggregate matches");
        assertEquals(0, metrics.poisonPillsTotal());
        assertEquals(0, metrics.unrecoverableFailuresTotal());
    }

    @Test
    void should_countPoisonPill_and_unrecoverable_underComponentName_when_batchHasPoisonPill() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        FakeOps ops = new FakeOps();
        BatchingPersistenceRunner<Item> runner = config(metrics, ops, 0).instantiate();

        // [good, POISON, good]: good(1) persists item-by-item, pill dead-lettered,
        // then the runner halts (unrecoverable). item 3 is abandoned.
        runner.persistBatch(List.of(
                PersistenceRunner.newEvent(new Item(1, false, false)),
                PersistenceRunner.newEvent(new Item(2, true, false)),
                PersistenceRunner.newEvent(new Item(3, false, false))));

        assertEquals(1, metrics.poisonPillsFor(ACTOR), "exactly one poison pill dead-lettered for this actor");
        assertEquals(1, metrics.unrecoverableFailuresFor(ACTOR), "halting at the pill records one unrecoverable failure");
        assertEquals(1, ops.deadLettered.get());
        assertEquals(1, metrics.eventsPersistedFor(ACTOR), "only the pre-pill good item is persisted");
    }

    @Test
    void should_countRetries_and_unrecoverable_when_transientRetriesExhausted() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();
        BatchingPersistenceRunner<Item> runner = config(metrics, new FakeOps(), 3).instantiate();

        // A single transient-failing item: 3 retries then unrecoverable.
        runner.persistBatch(List.of(PersistenceRunner.newEvent(new Item(1, false, true))));

        assertEquals(3, metrics.batchRetriesFor(ACTOR), "maxRetries retry attempts recorded for this actor");
        assertEquals(1, metrics.unrecoverableFailuresFor(ACTOR), "exhausted retries record one unrecoverable failure");
        assertEquals(0, metrics.batchesPersistedTotal(), "nothing committed");
    }

    @Test
    void should_countBackpressureEngageAndRelease_labelledByComponent_onWatermarkCrossings() {
        InMemoryBufferflyMetrics metrics = new InMemoryBufferflyMetrics();

        // A fake delegate whose size() we control via a counter; enqueue() is the
        // public entry point that triggers watermark evaluation on the real path.
        AtomicInteger depth = new AtomicInteger(0);
        PersistenceDispatcher<Item> delegate = new PersistenceDispatcher<>() {
            @Override public void enqueue(PersistenceRunner.PersistenceEvent<Item> e, PersistenceRunner<Item> r) { }
            @Override public void clear() { }
            @Override public int size() { return depth.get(); }
        };

        BackpressurePersistenceDispatcher<Item> bp = new BackpressurePersistenceDispatcher<>(
                delegate, 5, 2, BackpressureCallback.NO_OP, 0L, metrics, ACTOR);

        PersistenceRunner.PersistenceEvent<Item> probe = PersistenceRunner.newEvent(new Item(0, false, false));

        // Rising edge at high watermark (5) → engage once.
        depth.set(5);
        bp.enqueue(probe, null);
        depth.set(6);
        bp.enqueue(probe, null); // still engaged: idempotent
        assertEquals(1, metrics.backpressureEngagedFor(ACTOR));
        assertEquals(0, metrics.backpressureReleasedFor(ACTOR));

        // Dead-band (3): no new edge.
        depth.set(3);
        bp.enqueue(probe, null);
        assertEquals(1, metrics.backpressureEngagedFor(ACTOR));
        assertEquals(0, metrics.backpressureReleasedFor(ACTOR));

        // Falling edge at low watermark (2) → release once.
        depth.set(2);
        bp.enqueue(probe, null);
        depth.set(1);
        bp.enqueue(probe, null); // idempotent
        assertEquals(1, metrics.backpressureEngagedFor(ACTOR));
        assertEquals(1, metrics.backpressureReleasedFor(ACTOR));
        // And the aggregate agrees.
        assertEquals(1, metrics.backpressureEngagedTotal());
        assertEquals(1, metrics.backpressureReleasedTotal());
    }

    @Test
    void should_notThrow_when_runnerUsesNoopMetricsByDefault() {
        PersistenceConfig<Item> config = PersistenceConfig.builder(new FakeOps())
                .batchSize(10).timeoutMs(10L).maxRetries(0).build(); // NOOP metrics
        BatchingPersistenceRunner<Item> runner = config.instantiate();
        runner.persistBatch(List.of(PersistenceRunner.newEvent(new Item(1, false, false))));
        // No exception, nothing to assert beyond "it ran".
        assertTrue(true);
    }
}
