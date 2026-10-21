package io.bufferfly.spring.ledger;

import io.bufferfly.spring.BufferflySpringAutoConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 Spring + embedded Kafka: a poison pill in the stream is <b>isolated</b> by
 * the persistence runner (item-by-item), and <b>no message is lost</b> — not the
 * ones before the poison pill, nor the ones after.
 *
 * <p>Scenario (all for a single account, so they share one partition and are
 * processed in order by one {@link LedgerActor}):
 * <ol>
 *   <li>a run of good credits/debits <b>before</b> the poison pill;</li>
 *   <li>one <b>poison pill</b> — an {@link AddToAccount} whose {@code reason} is
 *       longer than the DB column allows;</li>
 *   <li>more good credits/debits <b>after</b> the poison pill.</li>
 * </ol>
 *
 * <p>The first time the batch containing the poison pill is flushed,
 * {@link LedgerOperations} throws {@code UnrecoverableException}. The runner's
 * {@code onUnrecoverable} hook calls {@code ctx.restartActor()}: the actor is
 * stopped and a fresh one started, whose {@code preStart} seeks back to the last
 * committed offset. On the replay the operations layer recognises the known-bad
 * record, dead-letters it, and lets everything else through — so the final
 * balance reflects <em>every</em> good message (before and after) exactly once,
 * and only the poison pill is dropped (into {@code dead_message}).
 */
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {LedgerShardFactory.TOPIC})
@Timeout(150)
@DisplayName("🦋 Actor restart + offset rewind on poison pill — no message lost before or after")
class LedgerRestartRewindTest {

    private static final String ACCOUNT = "ACC-REWIND";

    @Configuration
    @Import({BufferflySpringAutoConfiguration.class, LedgerShardFactory.class})
    static class TestConfig {

        @Bean
        DataSource dataSource() {
            String url = "jdbc:hsqldb:mem:ledger_" + UUID.randomUUID().toString().replace("-", "")
                    + ";sql.enforce_strict_size=true";
            return new DriverManagerDataSource(url, "SA", "");
        }

        @Bean
        LedgerOperations ledgerOperations(DataSource dataSource) {
            LedgerOperations ops = new LedgerOperations(dataSource);
            ops.createSchema();
            return ops;
        }

        @Bean
        BalanceConfirmationService balanceConfirmationService() {
            return new BalanceConfirmationService();
        }

        @Bean
        @SuppressWarnings({"unchecked", "rawtypes", "removal"})
        ConsumerFactory<Object, Object> consumerFactory(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            JsonDeserializer<AddToAccount> value = new JsonDeserializer<>(AddToAccount.class);
            value.addTrustedPackages("*");
            return new DefaultKafkaConsumerFactory(props, new StringDeserializer(), value);
        }
    }

    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private LedgerOperations operations;
    @Autowired
    private BalanceConfirmationService confirmations;
    @Autowired
    private LedgerShardFactory shardFactory;

    @Test
    @DisplayName("poison pill isolated, actor restarts & rewinds, no message lost or duplicated")
    void should_isolatePoisonPill_restartRewind_andLoseNoMessage() {
        // A reason longer than LedgerOperations.REASON_MAX_LENGTH (256).
        String oversizedReason = "x".repeat(LedgerOperations.REASON_MAX_LENGTH + 50);

        try (LedgerProducer ledger = new LedgerProducer(broker, LedgerShardFactory.TOPIC)) {
            // --- 5 good messages BEFORE the poison pill: +100 +100 +100 -20 -30 = 250 ---
            ledger.on(ACCOUNT)
                    .credit(100, "deposit-1")
                    .credit(100, "deposit-2")
                    .credit(100, "deposit-3")
                    .debit(20, "fee-1")
                    .debit(30, "fee-2");

            // --- the POISON PILL: a huge reason that cannot be persisted ---
            ledger.on(ACCOUNT).send(999, oversizedReason);

            // --- 4 good messages AFTER the poison pill: +500 -40 -60 +50 = 450 ---
            ledger.on(ACCOUNT)
                    .credit(500, "salary")
                    .debit(40, "groceries")
                    .debit(60, "gas")
                    .credit(50, "cashback");

            ledger.flush();

            // --- the SingleItemException path isolates the batch item-by-item
            //     (good records persist, the poison is dead-lettered and its
            //     offset is advanced), then HALTS the runner and fires the
            //     onUnrecoverable hook, which RESTARTS the actor. Its preStart
            //     rewinds to the committed offset, so processing resumes cleanly. ---
            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> assertTrue(shardFactory.restartCount() >= 1,
                            "the poison pill must halt the runner and trigger a restart + rewind"));

            // --- all 9 good changes persist exactly once (5 before + 4 after);
            //     none before or after the poison pill is lost, and the restart's
            //     rewind must not re-insert anything (no duplicates). ---
            await().atMost(Duration.ofSeconds(90))
                    .untilAsserted(() -> assertEquals(9, operations.countChanges(ACCOUNT),
                            "every good message before and after the poison pill must persist exactly once"));

            // hold to prove it settles at 9 (no duplicate inserts after rewind)
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> assertEquals(9, operations.countChanges(ACCOUNT),
                            "no duplicate inserts after the restart + rewind"));

            // --- final balance = 250 (before) + 450 (after) = 700; poison (+999) excluded ---
            assertEquals(700, operations.balanceOf(ACCOUNT),
                    "balance must reflect all good messages exactly once and exclude the poison pill");

            // --- the poison pill was dead-lettered, not silently dropped ---
            assertEquals(1, operations.countDeadMessages(),
                    "the poison pill must be dead-lettered exactly once");

            // --- confirmed read-model (post-commit side effect) agrees with the DB ---
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertEquals(700, confirmations.balanceOf(ACCOUNT)));
        }
    }
}
