package io.bufferfly.spring;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.metrics.BufferflyMetrics;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.TransactionalOperations;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link BufferflyHealthIndicator} that need no broker. They pin
 * the verdict logic: DOWN when a discovered container is not running, UP when
 * there are no containers, and that per-container {@code running} details are
 * reported. The "UP while actually consuming" path (which requires a live
 * container) is covered by {@link BufferflyHealthIndicatorIntegrationTest}.
 */
@Timeout(30)
class BufferflyHealthIndicatorTest {

    /** A no-op @ShardedActor factory so the post-processor can build a container. */
    @ShardedActor(name = "orders", topic = "orders-events", concurrency = 1,
            highWatermark = 100, lowWatermark = 10)
    static final class OrdersFactory implements ShardedActorFactory<String> {
        @Override
        public Actor<ConsumedRecord<String>> createActor(TopicPartition tp, KafkaContext ctx) {
            throw new UnsupportedOperationException("not needed: the container is never started in this test");
        }
    }

    static final class NoopOps implements TransactionalOperations<String> {
        @Override public long persistOnTransaction(List<String> batch, long t) { return t; }
        @Override public long onPoisonPill(String e, long t, Exception c) { return t; }
    }

    @SuppressWarnings("unchecked")
    private static ShardedActorBeanPostProcessor postProcessorWithBuiltContainer() {
        ConsumerFactory<Object, Object> cf = mock(ConsumerFactory.class);
        ShardedActorBeanPostProcessor pp = new ShardedActorBeanPostProcessor(cf, new ActorManager(), BufferflyMetrics.NOOP);
        // Discover an annotated factory and build (but DO NOT start) its container.
        pp.postProcessAfterInitialization(new OrdersFactory(), "ordersFactory");
        pp.buildContainers();
        return pp;
    }

    @Test
    void should_beDown_when_aContainerIsNotRunning() {
        ShardedActorBeanPostProcessor pp = postProcessorWithBuiltContainer();
        // Built but never started -> the underlying listener container is not running.
        assertTrue(pp.containers().stream().noneMatch(ShardedActorContainer::isRunning),
                "sanity: an unstarted container must report not running");

        Health health = new BufferflyHealthIndicator(pp).health();

        assertEquals(Status.DOWN, health.getStatus(), "a not-running container must make the indicator DOWN");
        Map<String, Object> details = health.getDetails();
        assertEquals("not running", details.get("orders"), "per-container running state is reported");
        assertEquals(1, details.get("containers"), "the container count is reported");
    }

    @Test
    void should_beUp_when_thereAreNoContainers() {
        @SuppressWarnings("unchecked")
        ConsumerFactory<Object, Object> cf = mock(ConsumerFactory.class);
        ShardedActorBeanPostProcessor pp =
                new ShardedActorBeanPostProcessor(cf, new ActorManager(), BufferflyMetrics.NOOP);
        // no @ShardedActor discovered, no containers built

        Health health = new BufferflyHealthIndicator(pp).health();

        assertEquals(Status.UP, health.getStatus(), "nothing to be unhealthy about");
        assertEquals(0, health.getDetails().get("containers"));
    }
}
