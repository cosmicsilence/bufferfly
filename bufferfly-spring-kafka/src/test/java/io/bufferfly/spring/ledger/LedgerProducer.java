package io.bufferfly.spring.ledger;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A tiny fluent DSL for publishing {@link AddToAccount} events to Kafka, so the
 * showcase test reads like a sequence of real bank operations:
 *
 * <pre>{@code
 * ledger.on("ACC-1").credit(1_000, "salary")
 *                    .debit(30,     "coffee")
 *                    .debit(12,     "lunch");
 * ledger.on("ACC-2").credit(500,    "refund");
 * ledger.flush();
 * }</pre>
 *
 * <p>The account number is used as the Kafka message key, so all events for an
 * account always land on the same partition and are therefore processed, in
 * order, by that partition's single {@code LedgerActor}.
 */
public final class LedgerProducer implements AutoCloseable {

    private final String topic;
    private final KafkaTemplate<String, AddToAccount> template;

    public LedgerProducer(EmbeddedKafkaBroker broker, String topic) {
        this.topic = topic;
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        ProducerFactory<String, AddToAccount> pf = new DefaultKafkaProducerFactory<>(props);
        this.template = new KafkaTemplate<>(pf);
    }

    /** Begins a fluent sequence of operations for one account. */
    public Account on(String accountNumber) {
        return new Account(accountNumber);
    }

    /** Blocks until all buffered records have been sent to the broker. */
    public void flush() {
        template.flush();
    }

    @Override
    public void close() {
        template.destroy();
    }

    /** Fluent per-account operations. Each call publishes one Kafka event. */
    public final class Account {
        private final String accountNumber;

        private Account(String accountNumber) {
            this.accountNumber = accountNumber;
        }

        /** Adds a positive amount (money in). */
        public Account credit(long amount, String reason) {
            return send(Math.abs(amount), reason);
        }

        /** Subtracts an amount (money out) — stored as a negative delta. */
        public Account debit(long amount, String reason) {
            return send(-Math.abs(amount), reason);
        }

        /** Sends a raw signed delta. */
        public Account send(long signedAmount, String reason) {
            template.send(topic, accountNumber, new AddToAccount(accountNumber, signedAmount, reason));
            return this;
        }

        /**
         * Floods this account's partition with {@code count} identical credits.
         * Because all events share the account key, they land on one partition
         * and are processed in order by a single {@code LedgerActor} — the way
         * to build a backlog against one actor's persistence buffer.
         *
         * @param count        how many events to publish
         * @param signedAmount the signed delta for each event
         * @param reason       the reason stamped on each event
         * @return this, for chaining
         */
        public Account flood(int count, long signedAmount, String reason) {
            for (int i = 0; i < count; i++) {
                template.send(topic, accountNumber, new AddToAccount(accountNumber, signedAmount, reason));
            }
            return this;
        }
    }
}
