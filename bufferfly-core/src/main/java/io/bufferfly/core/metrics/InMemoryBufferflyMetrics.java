package io.bufferfly.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * A dependency-free, thread-safe, in-memory {@link BufferflyMetrics}
 * implementation. It accumulates counters with {@link LongAdder}s — keyed by
 * component (actor / runner name) so per-component values can be inspected as
 * well as the aggregate — and keeps the registered gauge suppliers so their
 * current value can be sampled on demand.
 *
 * <p>Primarily a <b>reference implementation and test double</b>: tests wire it
 * into the engine and assert the counters/gauges move on the instrumented
 * paths, without pulling in any metrics library. It is also handy in production
 * for a quick {@code /debug/metrics} dump when a full backend is overkill.
 */
public final class InMemoryBufferflyMetrics implements BufferflyMetrics {

    private final Map<String, LongAdder> mailboxEnqueued = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> mailboxRefused = new ConcurrentHashMap<>();
    private final Map<String, IntSupplier> mailboxSizeGauges = new ConcurrentHashMap<>();

    private final Map<String, LongAdder> batchesPersisted = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> eventsPersisted = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> poisonPills = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> unrecoverableFailures = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> batchRetries = new ConcurrentHashMap<>();
    private final Map<String, IntSupplier> bufferDepthGauges = new ConcurrentHashMap<>();

    private final Map<String, LongAdder> backpressureEngaged = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> backpressureReleased = new ConcurrentHashMap<>();

    private final Map<String, LongAdder> partitionPaused = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> partitionResumed = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> actorRestarted = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> recordRejected = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> partitionAssigned = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> partitionRevoked = new ConcurrentHashMap<>();

    @Override
    public void mailboxEnqueued(String actorName) {
        inc(mailboxEnqueued, actorName);
    }

    @Override
    public void mailboxRefused(String actorName) {
        inc(mailboxRefused, actorName);
    }

    @Override
    public void registerMailboxSize(String actorName, IntSupplier sizeSupplier) {
        mailboxSizeGauges.put(actorName, sizeSupplier);
    }

    @Override
    public void batchPersisted(String component, int size) {
        inc(batchesPersisted, component);
        batchAdder(eventsPersisted, component).add(size);
    }

    @Override
    public void poisonPill(String component) {
        inc(poisonPills, component);
    }

    @Override
    public void unrecoverableFailure(String component) {
        inc(unrecoverableFailures, component);
    }

    @Override
    public void batchRetry(String component) {
        inc(batchRetries, component);
    }

    @Override
    public void registerPersistenceBufferDepth(String component, IntSupplier depthSupplier) {
        bufferDepthGauges.put(component, depthSupplier);
    }

    @Override
    public void backpressureEngaged(String component) {
        inc(backpressureEngaged, component);
    }

    @Override
    public void backpressureReleased(String component) {
        inc(backpressureReleased, component);
    }

    @Override
    public void partitionPaused(String component) {
        inc(partitionPaused, component);
    }

    @Override
    public void partitionResumed(String component) {
        inc(partitionResumed, component);
    }

    @Override
    public void actorRestarted(String component) {
        inc(actorRestarted, component);
    }

    @Override
    public void recordRejected(String component) {
        inc(recordRejected, component);
    }

    @Override
    public void partitionAssigned(String component) {
        inc(partitionAssigned, component);
    }

    @Override
    public void partitionRevoked(String component) {
        inc(partitionRevoked, component);
    }

    // -- read side (for assertions / dumps) ---------------------------------

    /** @return total messages enqueued across all mailboxes. */
    public long mailboxEnqueuedTotal() {
        return sum(mailboxEnqueued);
    }

    /** @return messages enqueued into {@code actorName}'s mailbox. */
    public long mailboxEnqueuedFor(String actorName) {
        return count(mailboxEnqueued, actorName);
    }

    /** @return total messages refused across all mailboxes. */
    public long mailboxRefusedTotal() {
        return sum(mailboxRefused);
    }

    /** @return messages refused by {@code actorName}'s mailbox. */
    public long mailboxRefusedFor(String actorName) {
        return count(mailboxRefused, actorName);
    }

    /** @return the current depth reported by {@code actorName}'s registered mailbox gauge, or -1 if none. */
    public int mailboxSize(String actorName) {
        IntSupplier g = mailboxSizeGauges.get(actorName);
        return g == null ? -1 : g.getAsInt();
    }

    /** @return {@code true} if a mailbox-size gauge was registered for {@code actorName}. */
    public boolean hasMailboxSizeGauge(String actorName) {
        return mailboxSizeGauges.containsKey(actorName);
    }

    public long batchesPersistedTotal() {
        return sum(batchesPersisted);
    }

    public long batchesPersistedFor(String component) {
        return count(batchesPersisted, component);
    }

    public long eventsPersistedTotal() {
        return sum(eventsPersisted);
    }

    public long eventsPersistedFor(String component) {
        return count(eventsPersisted, component);
    }

    public long poisonPillsTotal() {
        return sum(poisonPills);
    }

    public long poisonPillsFor(String component) {
        return count(poisonPills, component);
    }

    public long unrecoverableFailuresTotal() {
        return sum(unrecoverableFailures);
    }

    public long unrecoverableFailuresFor(String component) {
        return count(unrecoverableFailures, component);
    }

    public long batchRetriesTotal() {
        return sum(batchRetries);
    }

    public long batchRetriesFor(String component) {
        return count(batchRetries, component);
    }

    /** @return the current depth reported by {@code component}'s registered buffer gauge, or -1 if none. */
    public int persistenceBufferDepth(String component) {
        IntSupplier g = bufferDepthGauges.get(component);
        return g == null ? -1 : g.getAsInt();
    }

    /** @return {@code true} if a persistence-buffer-depth gauge was registered for {@code component}. */
    public boolean hasPersistenceBufferDepthGauge(String component) {
        return bufferDepthGauges.containsKey(component);
    }

    public long backpressureEngagedTotal() {
        return sum(backpressureEngaged);
    }

    public long backpressureEngagedFor(String component) {
        return count(backpressureEngaged, component);
    }

    public long backpressureReleasedTotal() {
        return sum(backpressureReleased);
    }

    public long backpressureReleasedFor(String component) {
        return count(backpressureReleased, component);
    }

    public long partitionPausedTotal() {
        return sum(partitionPaused);
    }

    public long partitionPausedFor(String component) {
        return count(partitionPaused, component);
    }

    public long partitionResumedTotal() {
        return sum(partitionResumed);
    }

    public long partitionResumedFor(String component) {
        return count(partitionResumed, component);
    }

    public long actorRestartedTotal() {
        return sum(actorRestarted);
    }

    public long actorRestartedFor(String component) {
        return count(actorRestarted, component);
    }

    public long recordRejectedTotal() {
        return sum(recordRejected);
    }

    public long recordRejectedFor(String component) {
        return count(recordRejected, component);
    }

    public long partitionAssignedTotal() {
        return sum(partitionAssigned);
    }

    public long partitionAssignedFor(String component) {
        return count(partitionAssigned, component);
    }

    public long partitionRevokedTotal() {
        return sum(partitionRevoked);
    }

    public long partitionRevokedFor(String component) {
        return count(partitionRevoked, component);
    }

    private static void inc(Map<String, LongAdder> m, String key) {
        batchAdder(m, key).increment();
    }

    private static LongAdder batchAdder(Map<String, LongAdder> m, String key) {
        return m.computeIfAbsent(key == null ? "unknown" : key, k -> new LongAdder());
    }

    private static long sum(Map<String, LongAdder> m) {
        long total = 0;
        for (LongAdder a : m.values()) total += a.sum();
        return total;
    }

    private static long count(Map<String, LongAdder> m, String key) {
        LongAdder a = m.get(key == null ? "unknown" : key);
        return a == null ? 0L : a.sum();
    }
}
