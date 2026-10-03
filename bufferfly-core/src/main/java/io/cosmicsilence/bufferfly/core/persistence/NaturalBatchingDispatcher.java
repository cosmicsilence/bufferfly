package io.cosmicsilence.bufferfly.core.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link PersistenceDispatcher} that groups buffered events into batches
 * using <em>natural batching</em> and executes them on a virtual thread.
 *
 * <h2>Natural batching</h2>
 * The consume loop drains up to {@code batchSize} events <em>or</em> waits up
 * to {@code timeoutMs} milliseconds — whichever comes first. Under high load
 * the batch is always full, maximising bulk-write efficiency. Under low load
 * the timeout ensures data is never held back indefinitely.
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
 * An unbounded {@link ConcurrentLinkedQueue} ensures {@link #enqueue} is
 * always non-blocking and never stalls the actor loop.
 */
public final class NaturalBatchingDispatcher<E> implements PersistenceDispatcher<E> {

    private final int batchSize;
    private final long timeoutMs;

    private final ConcurrentLinkedQueue<E> buffer = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final AtomicReference<Future<?>> runningTask = new AtomicReference<>(null);

    public NaturalBatchingDispatcher(int batchSize, long timeoutMs) {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        if (timeoutMs < 1 || timeoutMs > 50) throw new IllegalArgumentException("timeoutMs must be >= 1 AND <= 50 ms");
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void enqueue(E event, BatchingPersistenceRunner<E> runner) {
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

    private void tryConsumeSafely(BatchingPersistenceRunner<E> runner) {
        if (!scheduled.compareAndSet(false, true)) {
            return; // another thread is already scheduled or running
        }
        Future<?> task = executor.submit(() -> consumeLoop(runner));
        runningTask.set(task);
    }

    private void consumeLoop(BatchingPersistenceRunner<E> runner) {
        try {
            List<E> batch = drainBatch();
            if (!batch.isEmpty()) {
                runner.flush(batch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            scheduled.set(false);
            runningTask.set(null);
        }
        // Double-check: events may have arrived between the last buffer.poll()
        // and the scheduled.set(false) above. If so, reschedule immediately.
        if (!buffer.isEmpty()) {
            tryConsumeSafely(runner);
        }
    }

    private List<E> drainBatch() throws InterruptedException {
        List<E> batch = new ArrayList<>(batchSize);

        // Wait for the first event up to timeoutMs
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            E head = buffer.poll();
            if (head != null) {
                batch.add(head);
                break;
            }
            if (System.currentTimeMillis() >= deadline) {
                return batch; // nothing arrived within the window
            }
            Thread.sleep(10); // yield carrier thread rather than busy-spinning
        }

        // Greedily fill the remainder of the batch without waiting
        while (batch.size() < batchSize) {
            E next = buffer.poll();
            if (next == null) break;
            batch.add(next);
        }

        return batch;
    }
}
