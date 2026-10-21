package io.bufferfly.spring.ledger;

import io.bufferfly.spring.BufferflySpringAutoConfiguration;
import io.bufferfly.spring.ShardedActorContainer;
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
 * 🦋 Showcase: a sharded, persistent <b>ledger</b> on embedded Kafka, wired
 * entirely by <b>Spring</b> through the {@link io.bufferfly.spring.ShardedActor}
 * annotation.
 *
 * <p>Unlike a hand-wired test, here Spring instantiates everything:
 * <ul>
 *   <li>the application beans — {@link LedgerOperations},
 *       {@link BalanceConfirmationService}, the {@link ConsumerFactory}, and the
 *       {@code @ShardedActor}-annotated {@link LedgerShardFactory};</li>
 *   <li>BufferFly's {@link BufferflySpringAutoConfiguration}, which registers the
 *       default {@code ActorManager} and the {@code ShardedActorBeanPostProcessor}
 *       that discovers the annotated factory and stands up one
 *       {@link LedgerActor} per partition.</li>
 * </ul>
 *
 * <p>The test only produces events and asserts outcomes; it never constructs a
 * {@link ShardedActorContainer} itself.
 *
 * <p>What it demonstrates end-to-end: one actor per partition, in-memory
 * balance calc with batched persistence, append-only changes + updatable
 * balance, the Kafka offset stored as an optimistic-locking fencing token, and
 * post-commit side effects feeding a confirmation read-model.
 */
@SpringJUnitConfig
@EmbeddedKafka(partitions = 3, topics = {LedgerShardFactory.TOPIC})
@Timeout(120)
@DisplayName("🦋 Spring-wired sharded persistent ledger over embedded Kafka")
class LedgerShowcaseTest {

    /**
     * Application context: the business beans plus BufferFly's auto-configuration
     * and the annotated factory component. Everything below is built by Spring.
     */
    @Configuration
    @Import({BufferflySpringAutoConfiguration.class, LedgerShardFactory.class})
    static class TestConfig {

        /** A fresh in-memory HSQLDB per context. */
        @Bean
        DataSource dataSource() {
            String url = "jdbc:hsqldb:mem:ledger_" + UUID.randomUUID().toString().replace("-", "");
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

        /** The {@code ConsumerFactory} the BufferFly BPP injects to build consumers. */
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
    @DisplayName("balances, change history and fencing offset are all persisted correctly")
    void ledger_processes_events_and_persists_balances_changes_and_offset() {
        try (LedgerProducer ledger = new LedgerProducer(broker, LedgerShardFactory.TOPIC)) {

            // ACC-1: salary in, a few spends out  -> 1000 - 30 - 12 - 8 = 950
            ledger.on("ACC-1")
                    .credit(1_000, "salary")
                    .debit(30, "coffee")
                    .debit(12, "lunch")
                    .debit(8, "bus ticket");

            // ACC-2: a refund and a big purchase  -> 500 + 250 - 700 = 50
            ledger.on("ACC-2")
                    .credit(500, "refund")
                    .credit(250, "cashback")
                    .debit(700, "new phone");

            // ACC-3: paycheck then rent           -> 3000 - 1800 = 1200
            ledger.on("ACC-3")
                    .credit(3_000, "paycheck")
                    .debit(1_800, "rent");

            ledger.flush();

            // --- wait until every change has been durably persisted (4+3+2 = 9) ---
            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> assertEquals(9, operations.totalChanges(),
                            "all account changes must be persisted (no message loss)"));

            // --- final balances in the DB ---
            assertEquals(950, operations.balanceOf("ACC-1"), "ACC-1 balance");
            assertEquals(50, operations.balanceOf("ACC-2"), "ACC-2 balance");
            assertEquals(1_200, operations.balanceOf("ACC-3"), "ACC-3 balance");

            // --- append-only history keeps every movement ---
            assertEquals(4, operations.countChanges("ACC-1"));
            assertEquals(3, operations.countChanges("ACC-2"));
            assertEquals(2, operations.countChanges("ACC-3"));

            // --- confirmed balances (accounted by the POST-COMMIT side effect
            //     into a standalone BalanceConfirmationService) must match the DB. ---
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                assertEquals(950, confirmations.balanceOf("ACC-1"));
                assertEquals(50, confirmations.balanceOf("ACC-2"));
                assertEquals(1_200, confirmations.balanceOf("ACC-3"));
            });
            assertEquals(operations.balanceOf("ACC-1"), confirmations.balanceOf("ACC-1"),
                    "side-effect-confirmed balance must equal the persisted DB balance");

            // --- a fencing offset was stored for at least one partition ---
            assertTrue(operations.loadStoredOffset(LedgerShardFactory.TOPIC, partitionOf("ACC-1")) >= 0,
                    "a consumer offset must be durably stored as the fencing token");

            // --- healthy path: nothing dead-lettered, no zombie-fence restart ---
            assertEquals(0, operations.countDeadMessages(), "no poison pills on the happy path");
            assertTrue(!shardFactory.anyHalted(), "no zombie-fence restart on the happy path");
        }
    }

    private int partitionOf(String key) {
        return Math.floorMod(org.apache.kafka.common.utils.Utils.murmur2(key.getBytes()), 3);
    }
}
