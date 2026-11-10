package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.backpressure.UnderPressureException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ShardedActorConsumer} delivery and backpressure.
 */
@Timeout(20)
class ShardedActorConsumerTest {

    private static final TopicPartition TP = new TopicPartition("orders", 2);

    static final class FakeActorRef implements ActorReference<ConsumedRecord<String>> {
        final List<ConsumedRecord<String>> told = new ArrayList<>();
        RuntimeException toThrow;

        @Override
        public void tell(ConsumedRecord<String> r) {
            if (toThrow != null) throw toThrow;
            told.add(r);
        }

        @Override
        public void stop() {
        }
    }

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "k", value);
    }

    @Test
    void should_tellActor_onSuccess() {
        FakeActorRef actor = new FakeActorRef();
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);

        boolean accepted = consumer.tellActor(record(10, "v1"));

        assertTrue(accepted);
        assertEquals(1, actor.told.size());
        assertEquals("v1", actor.told.get(0).value());
        assertEquals(10L, actor.told.get(0).offset());
        assertTrue(paused.isEmpty(), "no pause when healthy");
    }

    @Test
    void should_rejectAndPause_onUnderPressure() {
        FakeActorRef actor = new FakeActorRef();
        actor.toThrow = new UnderPressureException("under pressure");
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);

        boolean accepted = consumer.tellActor(record(11, "v2"));

        assertFalse(accepted, "rejected -> the container does not commit the partition");
        assertEquals(List.of(TP), paused, "partition paused to let the system heal");
    }

    @Test
    void should_rejectAndPause_onMailBoxRefused() {
        FakeActorRef actor = new FakeActorRef();
        actor.toThrow = new MailBoxRefusedException("mailbox full");
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);

        boolean accepted = consumer.tellActor(record(12, "v3"));

        assertFalse(accepted);
        assertEquals(List.of(TP), paused);
    }
}
