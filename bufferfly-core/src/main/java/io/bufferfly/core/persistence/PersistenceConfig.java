package io.bufferfly.core.persistence;

/**
 * Configuration for persistent actors and {@link BatchingPersistenceRunner}.
 *
 * <p>Holds all parameters required by {@link BatchingPersistenceRunner} and is
 * responsible for instantiating the runner with the actor's side-effect processor,
 * breaking any cyclic dependency between the persistent actor and its runner.
 *
 * @param <E> the persistence event type
 */
public final class PersistenceConfig<E> {

    public static final int DEFAULT_BATCH_SIZE = 1_000;
    public static final long DEFAULT_TIMEOUT_MS = 20L;
    public static final int DEFAULT_MAX_RETRIES = 5;

    private final TransactionalOperations<E> operations;
    private final int batchSize;
    private final long timeoutMs;
    private final int maxRetries;
    private final Runnable onUnrecoverable;
    private final PersistenceDispatcher<E> dispatcher;

    private volatile BatchingPersistenceRunner<E> runner;

    public PersistenceConfig(TransactionalOperations<E> operations) {
        this(operations, DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS, DEFAULT_MAX_RETRIES, () -> {}, null);
    }

    public PersistenceConfig(TransactionalOperations<E> operations, int batchSize, long timeoutMs, int maxRetries) {
        this(operations, batchSize, timeoutMs, maxRetries, () -> {}, null);
    }

    public PersistenceConfig(
            TransactionalOperations<E> operations,
            int batchSize,
            long timeoutMs,
            int maxRetries,
            Runnable onUnrecoverable) {
        this(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, null);
    }

    public PersistenceConfig(
            TransactionalOperations<E> operations,
            int batchSize,
            long timeoutMs,
            int maxRetries,
            Runnable onUnrecoverable,
            PersistenceDispatcher<E> dispatcher) {
        if (operations == null) throw new NullPointerException("operations must not be null");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        if (timeoutMs < 1 || timeoutMs > 50)
            throw new IllegalArgumentException("timeoutMs must be >= 1 AND <= 50");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
        if (onUnrecoverable == null) throw new NullPointerException("onUnrecoverable must not be null");
        this.operations = operations;
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.maxRetries = maxRetries;
        this.onUnrecoverable = onUnrecoverable;
        this.dispatcher = dispatcher;
    }

    public static <E> PersistenceConfig<E> defaults(TransactionalOperations<E> operations) {
        return new PersistenceConfig<>(operations);
    }

    public static <E> Builder<E> builder(TransactionalOperations<E> operations) {
        return new Builder<>(operations);
    }

    /**
     * Instantiates the {@link BatchingPersistenceRunner} using this configuration
     * and the given {@link PersistenceRunner.SideEffectProcessor}.
     */
    public BatchingPersistenceRunner<E> instantiate(PersistenceRunner.SideEffectProcessor processor) {
        if (processor == null) throw new NullPointerException("processor must not be null");
        PersistenceDispatcher<E> disp = this.dispatcher != null
                ? this.dispatcher
                : new NaturalBatchingDispatcher<>(this.batchSize, this.timeoutMs);
        BatchingPersistenceRunner<E> instantiatedRunner = new BatchingPersistenceRunner<>(
                this.operations,
                this,
                this.onUnrecoverable,
                disp,
                processor
        );
        this.runner = instantiatedRunner;
        return instantiatedRunner;
    }

    public BatchingPersistenceRunner<E> instantiate() {
        return instantiate(se -> {});
    }

    public BatchingPersistenceRunner<E> createRunner(PersistenceRunner.SideEffectProcessor processor) {
        return instantiate(processor);
    }

    public BatchingPersistenceRunner<E> createRunner() {
        return instantiate();
    }

    public BatchingPersistenceRunner<E> buildRunner(PersistenceRunner.SideEffectProcessor processor) {
        return instantiate(processor);
    }

    public TransactionalOperations<E> operations() {
        return operations;
    }

    public int batchSize() {
        return batchSize;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public Runnable onUnrecoverable() {
        return onUnrecoverable;
    }

    public PersistenceDispatcher<E> dispatcher() {
        return dispatcher;
    }

    public BatchingPersistenceRunner<E> runner() {
        return runner;
    }

    public PersistenceConfig<E> withBatchSize(int batchSize) {
        return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
    }

    public PersistenceConfig<E> withTimeoutMs(long timeoutMs) {
        return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
    }

    public PersistenceConfig<E> withMaxRetries(int maxRetries) {
        return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
    }

    public PersistenceConfig<E> withOnUnrecoverable(Runnable onUnrecoverable) {
        return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
    }

    public PersistenceConfig<E> withDispatcher(PersistenceDispatcher<E> dispatcher) {
        return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
    }

    public static class Builder<E> {
        private final TransactionalOperations<E> operations;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private long timeoutMs = DEFAULT_TIMEOUT_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Runnable onUnrecoverable = () -> {};
        private PersistenceDispatcher<E> dispatcher;

        public Builder(TransactionalOperations<E> operations) {
            if (operations == null) throw new NullPointerException("operations must not be null");
            this.operations = operations;
        }

        public Builder<E> batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<E> timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder<E> maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder<E> onUnrecoverable(Runnable onUnrecoverable) {
            this.onUnrecoverable = onUnrecoverable;
            return this;
        }

        public Builder<E> dispatcher(PersistenceDispatcher<E> dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public PersistenceConfig<E> build() {
            return new PersistenceConfig<>(operations, batchSize, timeoutMs, maxRetries, onUnrecoverable, dispatcher);
        }
    }
}
