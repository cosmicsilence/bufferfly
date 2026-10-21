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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 🦋 Spring + embedded Kafka: <b>backpressure that pauses a partition</b>.
 *
 * <p>The showcase ledger normally keeps up trivially — in-memory HSQLDB drains a
 * batch almost as fast as it is enqueued, so the persistence buffer never grows
 * and backpressure never engages. To observe the mechanism we deliberately:
 * <ol>
 *   <li><b>shrink the watermarks</b> ({@code high=20}, {@code low=5}) and the
 *       batch size via {@link LedgerShardFactory#withBackpressure}, so a modest
 *       backlog is enough to cross the high watermark;</li>
 *   <li><b>throttle each bulk write</b> ({@link LedgerOperations#withPersistDelay})
 *       so persistence lags behind ingestion and the buffer actually fills;</li>
 *   <li><b>flood one partition</b> with a single account's events
 *       ({@link LedgerProducer.Account#flood}), so one actor's buffer is the one
 *       under pressure;</li>
 *   <li>use a <b>tiny evaluate interval</b>
 *       ({@link LedgerShardFactory#withEvaluateInterval}) so the backpressure
 *       dispatcher's periodic evaluator fires the <em>resume</em> edge promptly
 *       as the buffer drains — without it, a paused partition that stops
 *       enqueuing would never observe the buffer receding and never resume.</li>
 * </ol>
 *
 * <p>What we assert end-to-end:
 * <ul>
 *   <li>backpressure <b>engaged</b> — the high-watermark callback paused the
 *       partition at least once ({@code pauseCount >= 1});</li>
 *   <li>backpressure <b>released</b> — once the backlog drained below the low
 *       watermark the periodic evaluator resumed the partition, and nothing is
 *       left paused;</li>
 *   <li><b>no message loss</b> — every flooded event is durably persisted
 *       exactly once and the final balance is exactly right, proving the pause
 *       only told the broker to <em>wait</em>, never dropped anything.</li>
 * </ul>
 */
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {LedgerShardFactory.TOPIC})
@Timeout(180)
@DisplayName("🦋 Backpressure pauses a saturated partition and resumes it — no message lost")
class LedgerBackpressureTest {

    private static final String ACCOUNT = "ACC-FLOOD";
    private static final int FLOOD = 500;
    private static final long CREDIT = 2L;

    @Configuration
    @Import(BufferflySpringAutoConfiguration.class)
    static class TestConfig {

        @Bean
        DataSource dataSource() {
            String url = "jdbc:hsqldb:mem:ledger_" + UUID.randomUUID().toString().replace("-", "");
            return new DriverManagerDataSource(url, "SA", "");
        }

        @Bean
        LedgerOperations ledgerOperations(DataSource dataSource) {
            LedgerOperations ops = new LedgerOperations(dataSource);
            ops.createSchema();
            // Throttle each bulk write so persistence lags the flood and the
            // buffer backs up past the (shrunk) high watermark — engaging the
            // pause. Once the flood is drained the buffer recedes and the
            // dispatcher's periodic evaluator fires the resume.
            ops.withPersistDelay(40L);
            return ops;
        }

        @Bean
        BalanceConfirmationService balanceConfirmationService() {
            return new BalanceConfirmationService();
        }

        /**
         * The @ShardedActor factory, defined explicitly (rather than @Import-ed)
         * so we can shrink its watermarks here — during singleton instantiation,
         * before the BPP starts the container and the actor is created.
         */
        @Bean
        LedgerShardFactory ledgerShardFactory(LedgerOperations operations,
                                              BalanceConfirmationService confirmations) {
            return new LedgerShardFactory(operations, confirmations)
                    // small batch + tiny timeout + small watermarks so a modest
                    // backlog crosses the high watermark (pause).
                    .withBackpressure(5, 5L, 10, 2)
                    // tiny evaluation window so the falling edge (resume) fires
                    // promptly as the buffer drains after the flood — the whole
                    // point of the periodic evaluator. Production picks a larger
                    // value; the test uses a tiny one for a fast, deterministic
                    // transition.
                    .withEvaluateInterval(5L);
        }

        @Bean
        @SuppressWarnings({"unchecked", "rawtypes", "removal"})
        ConsumerFactory<Object, Object> consumerFactory(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            // Fetch aggressively so records pile into the buffer faster than the
            // throttled persistence can drain them, pushing it past the high
            // watermark.
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
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
    @DisplayName("high watermark pauses the partition, low watermark resumes it, and no event is lost")
    void backpressure_pauses_then_resumes_with_no_message_loss() {
        try (LedgerProducer ledger = new LedgerProducer(broker, LedgerShardFactory.TOPIC)) {

            // Flood a single account (=> single partition => single actor) with a
            // burst the throttled persistence cannot keep up with.
            ledger.on(ACCOUNT).flood(FLOOD, CREDIT, "flood");
            ledger.flush();

            // --- backpressure ENGAGED: the high watermark paused the partition ---
            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> assertTrue(shardFactory.pauseCount() >= 1,
                            "the persistence buffer must cross the high watermark and pause the partition"));

            // --- no message loss: every flooded event is eventually persisted ---
            await().atMost(Duration.ofSeconds(120))
                    .untilAsserted(() -> assertEquals(FLOOD, operations.countChanges(ACCOUNT),
                            "every flooded event must be durably persisted exactly once (pause != drop)"));

            // --- backpressure RELEASED: draining below the low watermark resumed
            //     the partition, and nothing is left paused at the end ---
            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> assertTrue(shardFactory.resumeCount() >= 1,
                            "once the backlog drained below the low watermark the partition must resume"));
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertFalse(shardFactory.anyPaused(),
                            "no partition may be left paused once the buffer has drained"));

            // --- final balance is exactly right (no loss, no duplicates) ---
            assertEquals(FLOOD * CREDIT, operations.balanceOf(ACCOUNT),
                    "balance must reflect all flooded credits exactly once");

            // --- confirmed read-model (post-commit side effect) agrees ---
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertEquals(FLOOD * CREDIT, confirmations.balanceOf(ACCOUNT)));

            // --- healthy path: nothing dead-lettered, no zombie-fence restart ---
            assertEquals(0, operations.countDeadMessages(), "no poison pills in a pure backpressure run");
            assertFalse(shardFactory.anyHalted(), "backpressure must not trigger an unrecoverable restart");
        }
    }
}
