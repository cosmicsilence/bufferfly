package io.bufferfly.core.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit sanity for the Kafka-lifecycle signals on {@link InMemoryBufferflyMetrics}:
 * each is accumulated per component and in aggregate. (The real instrumentation
 * wiring is exercised in the spring-kafka container tests; this pins the
 * collector's own bookkeeping.)
 */
class KafkaLifecycleMetricsTest {

    @Test
    void should_accumulatePerComponent_and_aggregate() {
        InMemoryBufferflyMetrics m = new InMemoryBufferflyMetrics();

        m.partitionPaused("a-0");
        m.partitionPaused("a-0");
        m.partitionResumed("a-0");
        m.actorRestarted("a-0");
        m.recordRejected("a-0");
        m.partitionAssigned("a-0");
        m.partitionRevoked("a-0");

        // a second component to prove labelling keeps series apart
        m.partitionPaused("b-1");
        m.recordRejected("b-1");
        m.recordRejected("b-1");

        assertEquals(2, m.partitionPausedFor("a-0"));
        assertEquals(1, m.partitionPausedFor("b-1"));
        assertEquals(3, m.partitionPausedTotal());

        assertEquals(1, m.partitionResumedFor("a-0"));
        assertEquals(1, m.actorRestartedFor("a-0"));

        assertEquals(1, m.recordRejectedFor("a-0"));
        assertEquals(2, m.recordRejectedFor("b-1"));
        assertEquals(3, m.recordRejectedTotal());

        assertEquals(1, m.partitionAssignedFor("a-0"));
        assertEquals(1, m.partitionRevokedFor("a-0"));
    }

    @Test
    void noop_isSilent() {
        BufferflyMetrics noop = BufferflyMetrics.NOOP;
        // must not throw
        noop.partitionPaused("x");
        noop.partitionResumed("x");
        noop.actorRestarted("x");
        noop.recordRejected("x");
        noop.partitionAssigned("x");
        noop.partitionRevoked("x");
    }
}
