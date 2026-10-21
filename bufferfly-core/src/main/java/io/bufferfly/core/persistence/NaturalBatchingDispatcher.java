package io.bufferfly.core.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
 * An effectively unbounded {@link LinkedBlockingQueue} (capacity
 * {@link Integer#MAX_VALUE}) ensures {@link #enqueue} is always non-blocking
 * and never stalls the actor loop. Its {@link LinkedBlockingQueue#size()} is
 * O(1) (backed by an internal counter), so {@link #size()} is cheap enough to
 * be polled on every enqueue by a backpressure decorator.
 */
public class NaturalBatchingDispatcher<E> implements PersistenceDispatcher<E> {

    private final int batchSize;
    private final long timeoutMs;

    private final LinkedBlockingQueue<PersistenceRunner.PersistenceEvent<E>> buffer = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
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
        Future<?> task = executor.submit(() -> consumeLoop(runner));
        runningTask.set(task);
    }

    private void consumeLoop(PersistenceRunner<E> runner) {
        try {
            List<PersistenceRunner.PersistenceEvent<E>> batch = drainBatch();
            if (!batch.isEmpty()) {
                runner.persistBatch(batch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            runningTask.set(null);
            scheduled.set(false);
        }
        // Double-check: events may have arrived between the last buffer.poll()
        // and the scheduled.set(false) above. If so, reschedule immediately.
        if (!buffer.isEmpty()) {
            tryConsumeSafely(runner);
        }
    }

    private List<PersistenceRunner.PersistenceEvent<E>> drainBatch() throws InterruptedException {
        List<PersistenceRunner.PersistenceEvent<E>> batch = new ArrayList<>(batchSize);

        // Wait for the first event up to timeoutMs
        long deadline = System.currentTimeMillis() + timeoutMs;

        int counter = 0 ;
        while (counter < batchSize) {
            PersistenceRunner.PersistenceEvent<E> head = buffer.poll();
            if (head != null) {
                counter = counter + 1;
                batch.add(head);
            } else {
                Thread.sleep(5);
            }
            if (System.currentTimeMillis() >= deadline) {
                break; // nothing arrived within the window
            }
        }
        // Greedily fill the remainder with the batch without waiting
        while (batch.size() < batchSize) {
            PersistenceRunner.PersistenceEvent<E> next = buffer.poll();
            if (next == null) break;
            batch.add(next);
        }
        return batch;
    }

}
