package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.MailBoxRefusedException;
import io.bufferfly.core.backpressure.UnderPressureException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ShardedActorConsumer} ack / no-message-loss / backpressure.
 */
@Timeout(20)
class ShardedActorConsumerTest {

    private static final TopicPartition TP = new TopicPartition("orders", 2);

    static final class FakeActorRef implements ActorReference<String> {
        final List<String> told = new ArrayList<>();
        RuntimeException toThrow;

        @Override
        public void tell(String s) {
            if (toThrow != null) throw toThrow;
            told.add(s);
        }

        @Override
        public void stop() {
        }
    }

    static final class CountingAck implements Acknowledgment {
        final AtomicInteger acks = new AtomicInteger(0);

        @Override
        public void acknowledge() {
            acks.incrementAndGet();
        }
    }

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "k", value);
    }

    @Test
    void should_tellActorThenAck_onSuccess() {
        FakeActorRef actor = new FakeActorRef();
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);
        CountingAck ack = new CountingAck();

        boolean accepted = consumer.accept(record(10, "v1"), ack);

        assertTrue(accepted);
        assertEquals(List.of("v1"), actor.told);
        assertEquals(1, ack.acks.get(), "offset acknowledged exactly once");
        assertTrue(paused.isEmpty(), "no pause when healthy");
    }

    @Test
    void should_notAckAndPause_onUnderPressure() {
        FakeActorRef actor = new FakeActorRef();
        actor.toThrow = new UnderPressureException("under pressure");
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);
        CountingAck ack = new CountingAck();

        boolean accepted = consumer.accept(record(11, "v2"), ack);

        assertFalse(accepted);
        assertEquals(0, ack.acks.get(), "no ack -> offset not committed -> will redeliver (no message loss)");
        assertEquals(List.of(TP), paused, "partition paused to let the system heal");
    }

    @Test
    void should_notAckAndPause_onMailBoxRefused() {
        FakeActorRef actor = new FakeActorRef();
        actor.toThrow = new MailBoxRefusedException("mailbox full");
        List<TopicPartition> paused = new ArrayList<>();
        var consumer = new ShardedActorConsumer<>(TP, actor, paused::add);
        CountingAck ack = new CountingAck();

        boolean accepted = consumer.accept(record(12, "v3"), ack);

        assertFalse(accepted);
        assertEquals(0, ack.acks.get(), "mailbox refusal must not ack -> redelivery");
        assertEquals(List.of(TP), paused);
    }
}
