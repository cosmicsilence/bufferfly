package io.bufferfly.spring;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.ConsumerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Tests that {@link ShardedActorBeanPostProcessor} reads {@link ShardedActor}
 * attributes, validates them, and rejects misconfigured beans — without
 * starting any container.
 */
@Timeout(20)
class ShardedActorBeanPostProcessorTest {

    @ShardedActor(name = "orders", topic = "orders-topic", concurrency = 5,
            highWatermark = 1000, lowWatermark = 100)
    static class GoodFactory implements ShardedActorFactory<String> {
        @Override
        public Actor<String> createActor(TopicPartition tp, KafkaContext ctx) {
            return null;
        }
    }

    @ShardedActor(name = "bad", topic = "t", highWatermark = 10, lowWatermark = 20)
    static class InvalidWatermarksFactory implements ShardedActorFactory<String> {
        @Override
        public Actor<String> createActor(TopicPartition tp, KafkaContext ctx) {
            return null;
        }
    }

    @ShardedActor(name = "notfactory", topic = "t")
    static class NotAFactory {
    }

    static class PlainBean {
    }

    @SuppressWarnings("unchecked")
    private static ShardedActorBeanPostProcessor newBpp() {
        return new ShardedActorBeanPostProcessor(mock(ConsumerFactory.class), new ActorManager());
    }

    @Test
    void should_passThroughUnannotatedBeans() {
        ShardedActorBeanPostProcessor bpp = newBpp();
        PlainBean bean = new PlainBean();
        assertSame(bean, bpp.postProcessAfterInitialization(bean, "plain"));
    }

    @Test
    void should_discoverAndReadAttributes_onAnnotatedFactory() {
        ShardedActorBeanPostProcessor bpp = newBpp();
        GoodFactory bean = new GoodFactory();

        Object returned = bpp.postProcessAfterInitialization(bean, "orders");

        assertSame(bean, returned, "BPP must return the original bean unchanged");
        // building (not starting) one container from the read attributes
        bpp.buildContainers();
        assertEquals(1, bpp.containers().size());
        assertEquals("orders", bpp.containers().get(0).name());
    }

    @Test
    void should_rejectAnnotatedBeanThatIsNotAFactory() {
        ShardedActorBeanPostProcessor bpp = newBpp();
        NotAFactory bean = new NotAFactory();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> bpp.postProcessAfterInitialization(bean, "notfactory"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("ShardedActorFactory"), ex.getMessage());
    }

    @Test
    void should_rejectInvalidWatermarks() {
        ShardedActorBeanPostProcessor bpp = newBpp();
        InvalidWatermarksFactory bean = new InvalidWatermarksFactory();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> bpp.postProcessAfterInitialization(bean, "bad"));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("highWatermark"), ex.getMessage());
    }
}
