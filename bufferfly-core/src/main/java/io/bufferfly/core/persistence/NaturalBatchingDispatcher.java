package io.bufferfly.core.persistence;

import io.bufferfly.core.metrics.BufferflyMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link PersistenceDispatcher} that groups buffered events into batches
 * using <em>natural batching</em> and executes them on a virtual thread.
 *
 * <h2>Natural batching</h2>
 * A batch starts as soon as events are buffered and is completed as soon as
 * {@code batchSize} is reached or the buffer is empty — it never waits for more
 * events. While a batch is being persisted, new events accumulate in the
 * buffer and form the next batch. Under load batches fill up to
 * {@code batchSize}; under light load each event is written as soon as the
 * previous batch commits.
 *
 * <h2>Thread-safety / single-writer guarantee</h2>
 * {@link #tryConsumeSafely} uses an {@link AtomicBoolean} as a mutex: the
 * thread that wins {@code compareAndSet(false, true)} is the <em>only</em>
 * one that submits a task to the executor. All other concurrent callers return
 * immediately without touching the executor. This means:
 * <ul>
 *   <li>No spurious task submissions.</li>
 *   <li>No tasks started and immediately cancelled.</li>
 *   <li>Exactly one VT is active at any time (single-writer).</li>
 * </ul>
 * The {@link AtomicReference} for the {@link Future} is kept separately and is
 * only needed for interrupting the in-flight task during {@link #clear()}.
 *
 * <h2>Buffer</h2>
 * An effectively unbounded {@link LinkedBlockingQueue} (capacity
 * {@link Integer#MAX_VALUE}) ensures {@link #enqueue} is always non-blocking
 * and never stalls the actor loop. Its {@link LinkedBlockingQueue#size()} is
 * O(1) (backed by an internal counter), so {@link #size()} is cheap enough to
 * be polled on every enqueue by a backpressure decorator.
 */
public class NaturalBatchingDispatcher<E> implements PersistenceDispatcher<E> {

    /** Default label for this dispatcher's buffer-depth gauge. */
    public static final String DEFAULT_COMPONENT_NAME = "bufferfly-persistence";

    private final int batchSize;

    private final LinkedBlockingQueue<PersistenceRunner.PersistenceEvent<E>> buffer = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final AtomicReference<Future<?>> runningTask = new AtomicReference<>(null);

    public NaturalBatchingDispatcher(int batchSize) {
        this(batchSize, BufferflyMetrics.NOOP);
    }

    /**
     * Creates a natural-batching dispatcher that reports its buffer depth to
     * {@code metrics}. The depth gauge is registered once here (the backend
     * samples {@link #size()} on its own schedule), under the default component
     * name {@value #DEFAULT_COMPONENT_NAME}.
     *
     * @param batchSize maximum events coalesced per batch
     * @param metrics   observability sink; {@link BufferflyMetrics#NOOP} to disable
     */
    public NaturalBatchingDispatcher(int batchSize, BufferflyMetrics metrics) {
        this(batchSize, metrics, DEFAULT_COMPONENT_NAME);
    }

    /**
     * Full constructor: as {@link #NaturalBatchingDispatcher(int, BufferflyMetrics)}
     * but with a caller-supplied {@code componentName} used to label the
     * buffer-depth gauge, so several dispatchers can be told apart.
     *
     * @param batchSize     maximum events coalesced per batch
     * @param metrics       observability sink; {@link BufferflyMetrics#NOOP} to disable
     * @param componentName label for the buffer-depth gauge
     */
    public NaturalBatchingDispatcher(int batchSize, BufferflyMetrics metrics, String componentName) {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        this.batchSize = batchSize;
        BufferflyMetrics m = metrics == null ? BufferflyMetrics.NOOP : metrics;
        // Register the buffer-depth gauge once; the backend pulls size() itself.
        m.registerPersistenceBufferDepth(
                componentName == null ? DEFAULT_COMPONENT_NAME : componentName, this::size);
    }

    @Override
    public void enqueue(PersistenceRunner.PersistenceEvent<E> event, PersistenceRunner<E> runner) {
        buffer.offer(event);
        tryConsumeSafely(runner);
    }

    @Override
    public void clear() {
        scheduled.set(false);
        Optional
                .ofNullable(runningTask.getAndSet(null))
                .ifPresent(f -> f.cancel(true));
        buffer.clear();
    }

    @Override
    public int size() {
        return buffer.size();
    }

    private void tryConsumeSafely(PersistenceRunner<E> runner) {
        if (!scheduled.compareAndSet(false, true)) {
            return; // another thread is already scheduled or running
        }
        final FutureTask<Void> task = new FutureTask<>(() -> {
            consumeLoop(runner);
            return null;
        });
        runningTask.set(task);
        executor.submit(task);
    }

    private void consumeLoop(PersistenceRunner<E> runner) {
        try {
            List<PersistenceRunner.PersistenceEvent<E>> batch = new ArrayList<>(batchSize);
            buffer.drainTo(batch, batchSize);
            if (!batch.isEmpty()) {
                runner.persistBatch(batch);
            }
        } finally {
            runningTask.set(null);
            scheduled.set(false);
        }
        // Events may have arrived while the batch was persisted, or after the
        // drain and before scheduled.set(false). If so, reschedule immediately.
        if (!buffer.isEmpty()) {
            tryConsumeSafely(runner);
        }
    }
}
