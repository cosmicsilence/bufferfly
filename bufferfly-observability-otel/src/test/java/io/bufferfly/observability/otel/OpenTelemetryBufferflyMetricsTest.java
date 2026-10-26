package io.bufferfly.observability.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link OpenTelemetryBufferflyMetrics} records the expected
 * OpenTelemetry instruments. A real {@link SdkMeterProvider} with an
 * {@link InMemoryMetricReader} is wired up; after driving the SPI, a collection
 * is triggered and the resulting {@link MetricData} is asserted — including that
 * asynchronous gauges sample their registered suppliers at collection time and
 * that per-component attributes are attached.
 */
class OpenTelemetryBufferflyMetricsTest {

    private InMemoryMetricReader reader;
    private SdkMeterProvider meterProvider;
    private OpenTelemetryBufferflyMetrics metrics;

    @BeforeEach
    void setUp() {
        reader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        Meter meter = meterProvider.get("io.bufferfly.test");
        metrics = new OpenTelemetryBufferflyMetrics(meter);
    }

    @AfterEach
    void tearDown() {
        metrics.close();
        meterProvider.close();
    }

    @Test
    void should_recordCounters_withExpectedValues() {
        metrics.mailboxEnqueued("actor-a");
        metrics.mailboxEnqueued("actor-a");
        metrics.mailboxEnqueued("actor-b");
        metrics.mailboxRefused("actor-a");

        metrics.batchPersisted("runner-x", 5);     // 1 batch, 5 events
        metrics.batchPersisted("runner-x", 3);     // 1 batch, 3 events
        metrics.poisonPill("runner-x");
        metrics.unrecoverableFailure("runner-x");
        metrics.batchRetry("runner-x");
        metrics.batchRetry("runner-x");

        metrics.backpressureEngaged("runner-x");
        metrics.backpressureReleased("runner-x");

        metrics.partitionPaused("runner-x");
        metrics.partitionResumed("runner-x");
        metrics.actorRestarted("runner-x");
        metrics.recordRejected("runner-x");
        metrics.recordRejected("runner-x");
        metrics.partitionAssigned("runner-x");
        metrics.partitionRevoked("runner-x");

        Collection<MetricData> metricsData = reader.collectAllMetrics();

        // Counters sum to the number of recorded events.
        assertEquals(3, sumLong(metricsData, OpenTelemetryBufferflyMetrics.MAILBOX_ENQUEUED),
                "two for actor-a + one for actor-b");
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.MAILBOX_REFUSED));
        assertEquals(2, sumLong(metricsData, OpenTelemetryBufferflyMetrics.BATCHES_PERSISTED));
        assertEquals(8, sumLong(metricsData, OpenTelemetryBufferflyMetrics.EVENTS_PERSISTED), "5 + 3");
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.POISON_PILLS));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.UNRECOVERABLE_FAILURES));
        assertEquals(2, sumLong(metricsData, OpenTelemetryBufferflyMetrics.BATCH_RETRIES));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.BACKPRESSURE_ENGAGED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.BACKPRESSURE_RELEASED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.PARTITION_PAUSED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.PARTITION_RESUMED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.ACTOR_RESTARTED));
        assertEquals(2, sumLong(metricsData, OpenTelemetryBufferflyMetrics.RECORD_REJECTED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.PARTITION_ASSIGNED));
        assertEquals(1, sumLong(metricsData, OpenTelemetryBufferflyMetrics.PARTITION_REVOKED));
    }

    @Test
    void should_attachActorAttribute_toMailboxCounters() {
        metrics.mailboxEnqueued("payment-actor");
        metrics.mailboxEnqueued("payment-actor");

        Collection<MetricData> metricsData = reader.collectAllMetrics();
        MetricData enqueued = byName(metricsData, OpenTelemetryBufferflyMetrics.MAILBOX_ENQUEUED);
        assertNotNull(enqueued, "enqueued counter must be present");

        List<LongPointData> points = List.copyOf(enqueued.getLongSumData().getPoints());
        assertEquals(1, points.size(), "one series for the single actor");
        LongPointData p = points.get(0);
        assertEquals(2, p.getValue());
        assertEquals("payment-actor", p.getAttributes().get(AttributeKey.stringKey("actor")),
                "the actor name must be attached as the 'actor' attribute");
    }

    @Test
    void should_attachComponentAttribute_toPersistenceAndBackpressureCounters() {
        metrics.batchPersisted("ledger-actor-2", 4);
        metrics.poisonPill("ledger-actor-2");
        metrics.backpressureEngaged("ledger-actor-2");

        Collection<MetricData> metricsData = reader.collectAllMetrics();

        for (String name : List.of(
                OpenTelemetryBufferflyMetrics.BATCHES_PERSISTED,
                OpenTelemetryBufferflyMetrics.POISON_PILLS,
                OpenTelemetryBufferflyMetrics.BACKPRESSURE_ENGAGED)) {
            MetricData md = byName(metricsData, name);
            assertNotNull(md, name + " must be present");
            LongPointData point = md.getLongSumData().getPoints().iterator().next();
            assertEquals("ledger-actor-2", point.getAttributes().get(AttributeKey.stringKey("component")),
                    name + " must carry the owning component as the 'component' attribute");
        }
    }

    @Test
    void should_sampleMailboxSizeGauge_fromRegisteredSupplier_atCollectionTime() {
        AtomicInteger depth = new AtomicInteger(7);
        metrics.registerMailboxSize("gauge-actor", depth::get);

        // First collection samples the supplier → 7.
        assertEquals(7, gaugeValue(reader.collectAllMetrics(), OpenTelemetryBufferflyMetrics.MAILBOX_SIZE));

        // The async gauge re-samples on the next collection → 3.
        depth.set(3);
        assertEquals(3, gaugeValue(reader.collectAllMetrics(), OpenTelemetryBufferflyMetrics.MAILBOX_SIZE));
    }

    @Test
    void should_sampleBufferDepthGauge_withComponentAttribute() {
        AtomicInteger depth = new AtomicInteger(42);
        metrics.registerPersistenceBufferDepth("bufferfly-persistence", depth::get);

        Collection<MetricData> metricsData = reader.collectAllMetrics();
        MetricData gauge = byName(metricsData, OpenTelemetryBufferflyMetrics.BUFFER_DEPTH);
        assertNotNull(gauge, "buffer-depth gauge must be present");

        List<LongPointData> points = List.copyOf(gauge.getLongGaugeData().getPoints());
        assertEquals(1, points.size());
        LongPointData p = points.get(0);
        assertEquals(42, p.getValue());
        assertEquals("bufferfly-persistence", p.getAttributes().get(AttributeKey.stringKey("component")));
    }

    @Test
    void should_unregisterGaugeCallbacks_onClose() {
        metrics.registerMailboxSize("closing-actor", () -> 9);
        assertEquals(9, gaugeValue(reader.collectAllMetrics(), OpenTelemetryBufferflyMetrics.MAILBOX_SIZE));

        metrics.close();

        // After close, the async gauge callback is unregistered → no mailbox-size series.
        MetricData gauge = byName(reader.collectAllMetrics(), OpenTelemetryBufferflyMetrics.MAILBOX_SIZE);
        boolean absentOrEmpty = gauge == null || gauge.getLongGaugeData().getPoints().isEmpty();
        assertTrue(absentOrEmpty, "the mailbox-size gauge must be gone after close()");
    }

    // -- helpers ------------------------------------------------------------

    private static MetricData byName(Collection<MetricData> data, String name) {
        return data.stream().filter(m -> m.getName().equals(name)).findFirst().orElse(null);
    }

    private static long sumLong(Collection<MetricData> data, String name) {
        MetricData md = byName(data, name);
        if (md == null) return 0L;
        return md.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).sum();
    }

    private static long gaugeValue(Collection<MetricData> data, String name) {
        MetricData md = byName(data, name);
        assertNotNull(md, "gauge " + name + " must be present");
        return md.getLongGaugeData().getPoints().stream().mapToLong(LongPointData::getValue).findFirst().orElseThrow();
    }
}
