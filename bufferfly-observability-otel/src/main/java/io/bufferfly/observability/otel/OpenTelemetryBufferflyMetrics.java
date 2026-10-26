package io.bufferfly.observability.otel;

import io.bufferfly.core.metrics.BufferflyMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

/**
 * An OpenTelemetry-backed {@link BufferflyMetrics}: maps BufferFly's
 * dependency-free observability SPI onto OpenTelemetry instruments.
 *
 * <h2>Mapping</h2>
 * <ul>
 *   <li>{@code mailboxEnqueued}/{@code mailboxRefused} → {@code LongCounter}
 *       ({@value #MAILBOX_ENQUEUED}, {@value #MAILBOX_REFUSED}).</li>
 *   <li>{@code registerMailboxSize} → asynchronous {@code ObservableLongGauge}
 *       ({@value #MAILBOX_SIZE}).</li>
 *   <li>{@code batchPersisted} → two {@code LongCounter}s
 *       ({@value #BATCHES_PERSISTED}, {@value #EVENTS_PERSISTED}).</li>
 *   <li>{@code poisonPill}/{@code unrecoverableFailure}/{@code batchRetry} →
 *       {@code LongCounter}s ({@value #POISON_PILLS}, {@value #UNRECOVERABLE_FAILURES},
 *       {@value #BATCH_RETRIES}).</li>
 *   <li>{@code registerPersistenceBufferDepth} → asynchronous
 *       {@code ObservableLongGauge} ({@value #BUFFER_DEPTH}).</li>
 *   <li>{@code backpressureEngaged}/{@code backpressureReleased} →
 *       {@code LongCounter}s ({@value #BACKPRESSURE_ENGAGED}, {@value #BACKPRESSURE_RELEASED}).</li>
 * </ul>
 *
 * <p>Per-component values carry the actor/runner name: mailbox instruments use
 * an {@code actor} attribute, and persistence/backpressure instruments use a
 * {@code component} attribute, so a backend can split the series per component.
 *
 * <h2>Gauges</h2>
 * Gauge registration builds an asynchronous {@code ObservableLongGauge} whose
 * callback samples the supplied {@link IntSupplier} whenever the SDK's metric
 * reader collects — this is OpenTelemetry's idiomatic way to observe a
 * continuously-varying value without pushing on the hot path. The returned
 * {@link ObservableLongGauge} handles are retained so {@link #close()} can
 * unregister every callback.
 *
 * <h2>Lifecycle</h2>
 * The caller owns the OpenTelemetry SDK and passes in a {@link Meter}. This
 * adapter never touches the SDK's export/shutdown; call {@link #close()} to
 * unregister the asynchronous gauge callbacks this instance created.
 *
 * <p>All counter methods are safe to call from any thread (OTel instruments are
 * thread-safe) and never throw.
 */
public final class OpenTelemetryBufferflyMetrics implements BufferflyMetrics, AutoCloseable {

    public static final String MAILBOX_ENQUEUED = "bufferfly.mailbox.enqueued";
    public static final String MAILBOX_REFUSED = "bufferfly.mailbox.refused";
    public static final String MAILBOX_SIZE = "bufferfly.mailbox.size";
    public static final String BATCHES_PERSISTED = "bufferfly.persistence.batches";
    public static final String EVENTS_PERSISTED = "bufferfly.persistence.events";
    public static final String POISON_PILLS = "bufferfly.persistence.poison_pills";
    public static final String UNRECOVERABLE_FAILURES = "bufferfly.persistence.unrecoverable";
    public static final String BATCH_RETRIES = "bufferfly.persistence.retries";
    public static final String BUFFER_DEPTH = "bufferfly.persistence.buffer_depth";
    public static final String BACKPRESSURE_ENGAGED = "bufferfly.backpressure.engaged";
    public static final String BACKPRESSURE_RELEASED = "bufferfly.backpressure.released";
    public static final String PARTITION_PAUSED = "bufferfly.partition.paused";
    public static final String PARTITION_RESUMED = "bufferfly.partition.resumed";
    public static final String ACTOR_RESTARTED = "bufferfly.actor.restarted";
    public static final String RECORD_REJECTED = "bufferfly.record.rejected";
    public static final String PARTITION_ASSIGNED = "bufferfly.partition.assigned";
    public static final String PARTITION_REVOKED = "bufferfly.partition.revoked";

    static final AttributeKey<String> ACTOR_ATTR = AttributeKey.stringKey("actor");
    static final AttributeKey<String> COMPONENT_ATTR = AttributeKey.stringKey("component");

    private final LongCounter mailboxEnqueued;
    private final LongCounter mailboxRefused;
    private final LongCounter batchesPersisted;
    private final LongCounter eventsPersisted;
    private final LongCounter poisonPills;
    private final LongCounter unrecoverableFailures;
    private final LongCounter batchRetries;
    private final LongCounter backpressureEngaged;
    private final LongCounter backpressureReleased;
    private final LongCounter partitionPaused;
    private final LongCounter partitionResumed;
    private final LongCounter actorRestarted;
    private final LongCounter recordRejected;
    private final LongCounter partitionAssigned;
    private final LongCounter partitionRevoked;

    private final Meter meter;

    /** Retained async-gauge handles so {@link #close()} can unregister them. */
    private final ConcurrentHashMap<String, ObservableLongGauge> mailboxGauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ObservableLongGauge> bufferGauges = new ConcurrentHashMap<>();

    /**
     * @param meter the OpenTelemetry {@link Meter} to build instruments on
     *              (typically {@code openTelemetry.getMeter("io.bufferfly")})
     */
    public OpenTelemetryBufferflyMetrics(Meter meter) {
        this.meter = Objects.requireNonNull(meter, "meter must not be null");
        this.mailboxEnqueued = meter.counterBuilder(MAILBOX_ENQUEUED)
                .setDescription("Messages accepted into actor mailboxes").setUnit("{message}").build();
        this.mailboxRefused = meter.counterBuilder(MAILBOX_REFUSED)
                .setDescription("Messages refused by full actor mailboxes").setUnit("{message}").build();
        this.batchesPersisted = meter.counterBuilder(BATCHES_PERSISTED)
                .setDescription("Persistence batches committed").setUnit("{batch}").build();
        this.eventsPersisted = meter.counterBuilder(EVENTS_PERSISTED)
                .setDescription("Persistence events committed").setUnit("{event}").build();
        this.poisonPills = meter.counterBuilder(POISON_PILLS)
                .setDescription("Poison pills dead-lettered").setUnit("{pill}").build();
        this.unrecoverableFailures = meter.counterBuilder(UNRECOVERABLE_FAILURES)
                .setDescription("Unrecoverable persistence failures (restart hook fired)").setUnit("{failure}").build();
        this.batchRetries = meter.counterBuilder(BATCH_RETRIES)
                .setDescription("Transient batch-persist retry attempts").setUnit("{attempt}").build();
        this.backpressureEngaged = meter.counterBuilder(BACKPRESSURE_ENGAGED)
                .setDescription("Backpressure engage transitions").setUnit("{transition}").build();
        this.backpressureReleased = meter.counterBuilder(BACKPRESSURE_RELEASED)
                .setDescription("Backpressure release transitions").setUnit("{transition}").build();
        this.partitionPaused = meter.counterBuilder(PARTITION_PAUSED)
                .setDescription("Kafka partition pauses").setUnit("{pause}").build();
        this.partitionResumed = meter.counterBuilder(PARTITION_RESUMED)
                .setDescription("Kafka partition resumes").setUnit("{resume}").build();
        this.actorRestarted = meter.counterBuilder(ACTOR_RESTARTED)
                .setDescription("Actor restarts after unrecoverable failure").setUnit("{restart}").build();
        this.recordRejected = meter.counterBuilder(RECORD_REJECTED)
                .setDescription("Records left un-acked for redelivery").setUnit("{record}").build();
        this.partitionAssigned = meter.counterBuilder(PARTITION_ASSIGNED)
                .setDescription("Kafka partition assignments").setUnit("{assignment}").build();
        this.partitionRevoked = meter.counterBuilder(PARTITION_REVOKED)
                .setDescription("Kafka partition revocations").setUnit("{revocation}").build();
    }

    // -- mailbox / dispatch -------------------------------------------------

    @Override
    public void mailboxEnqueued(String actorName) {
        mailboxEnqueued.add(1, Attributes.of(ACTOR_ATTR, safe(actorName)));
    }

    @Override
    public void mailboxRefused(String actorName) {
        mailboxRefused.add(1, Attributes.of(ACTOR_ATTR, safe(actorName)));
    }

    @Override
    public void registerMailboxSize(String actorName, IntSupplier sizeSupplier) {
        String name = safe(actorName);
        // Replace-and-close if the same actor re-registers (idempotent, no leak).
        mailboxGauges.compute(name, (k, previous) -> {
            if (previous != null) previous.close();
            Attributes attrs = Attributes.of(ACTOR_ATTR, name);
            return meter.gaugeBuilder(MAILBOX_SIZE)
                    .setDescription("Current actor mailbox depth").setUnit("{message}")
                    .ofLongs()
                    .buildWithCallback(m -> m.record(sizeSupplier.getAsInt(), attrs));
        });
    }

    // -- persistence runner -------------------------------------------------

    @Override
    public void batchPersisted(String component, int size) {
        Attributes attrs = Attributes.of(COMPONENT_ATTR, safe(component));
        batchesPersisted.add(1, attrs);
        eventsPersisted.add(size, attrs);
    }

    @Override
    public void poisonPill(String component) {
        poisonPills.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void unrecoverableFailure(String component) {
        unrecoverableFailures.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void batchRetry(String component) {
        batchRetries.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void registerPersistenceBufferDepth(String component, IntSupplier depthSupplier) {
        String name = safe(component);
        bufferGauges.compute(name, (k, previous) -> {
            if (previous != null) previous.close();
            Attributes attrs = Attributes.of(COMPONENT_ATTR, name);
            return meter.gaugeBuilder(BUFFER_DEPTH)
                    .setDescription("Current persistence buffer depth").setUnit("{event}")
                    .ofLongs()
                    .buildWithCallback(m -> m.record(depthSupplier.getAsInt(), attrs));
        });
    }

    // -- backpressure -------------------------------------------------------

    @Override
    public void backpressureEngaged(String component) {
        backpressureEngaged.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void backpressureReleased(String component) {
        backpressureReleased.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    // -- kafka partition lifecycle ------------------------------------------

    @Override
    public void partitionPaused(String component) {
        partitionPaused.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void partitionResumed(String component) {
        partitionResumed.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void actorRestarted(String component) {
        actorRestarted.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void recordRejected(String component) {
        recordRejected.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void partitionAssigned(String component) {
        partitionAssigned.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    @Override
    public void partitionRevoked(String component) {
        partitionRevoked.add(1, Attributes.of(COMPONENT_ATTR, safe(component)));
    }

    // -- lifecycle ----------------------------------------------------------

    /** Unregisters every asynchronous gauge callback this instance created. */
    @Override
    public void close() {
        mailboxGauges.values().forEach(ObservableLongGauge::close);
        mailboxGauges.clear();
        bufferGauges.values().forEach(ObservableLongGauge::close);
        bufferGauges.clear();
    }

    private static String safe(String s) {
        return s == null ? "unknown" : s;
    }
}
