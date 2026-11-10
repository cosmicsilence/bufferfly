package io.bufferfly.spring.benchmark.flink;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.bufferfly.spring.benchmark.BenchmarkConsumers;
import io.bufferfly.spring.benchmark.BenchmarkProducer;
import io.bufferfly.spring.benchmark.LedgerTopic;
import io.bufferfly.spring.benchmark.PgActorLedgerOperations;
import io.bufferfly.spring.benchmark.PgLedgerStore;
import io.bufferfly.spring.benchmark.PgStats;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 🦋 <b>Benchmark</b>: BufferFly sharded actors vs. <b>Apache Flink</b> on the
 * ledger workload — same Kafka topic, same Postgres tables, same batch size,
 * state kept in memory on both sides.
 *
 * <ul>
 *   <li><b>Flink</b> ({@link FlinkLedgerJob}): bounded {@code KafkaSource} →
 *       {@code keyBy(account)} → {@code KeyedProcessFunction} with the balance in
 *       keyed {@code ValueState} (heap state backend, the default) → the JDBC
 *       connector's {@code JdbcSink} at-least-once, JDBC batches of
 *       {@link #BATCH_SIZE}. Standard local settings otherwise; run at Flink's
 *       default parallelism (one subtask per core) and at {@link #FLINK_SMALL_PARALLELISM}
 *       to match the actor side's number of writers.</li>
 *   <li><b>BufferFly</b> ({@link ShardedLedgerRunner}): one
 *       {@code ShardedActorContainer} with one consumer per partition, one
 *       persistent actor per partition, balances in a {@code HashMap}, natural
 *       batching up to {@link #BATCH_SIZE} changes per transaction.</li>
 * </ul>
 * One topic with {@link #PARTITIONS} partitions; records are keyed by account,
 * so Kafka's default partitioner decides each account's partition (the split is
 * not perfectly even and is reported per scenario). Both poll
 * {@link BenchmarkConsumers#MAX_POLL_RECORDS} records at a time.
 *
 * <h2>Fairness</h2>
 * <ul>
 *   <li>Each scenario's workload is produced once to its own topic; each run
 *       reads it from the beginning with its own consumer group and freshly
 *       created tables.</li>
 *   <li>Rounds rotate which approach runs first; medians are reported.</li>
 *   <li>Two times per run: from start (includes Flink's local cluster start-up
 *       and job submission, or the container's consumer-group join) and from the
 *       first record processed to the last change committed (steady state).</li>
 *   <li>Every run must persist every change exactly once with each account
 *       holding its exact final balance, and no change row may be out of Kafka
 *       offset order.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(3600)
@DisplayName("🦋 Benchmark: sharded actors vs Apache Flink (keyed state + JDBC sink) over Postgres + Kafka")
class FlinkVsShardedActorLedgerBenchmarkTest {

    private static final int PARTITIONS = 3;
    private static final int MESSAGES = 600_000;
    private static final int BATCH_SIZE = BenchmarkConsumers.MAX_POLL_RECORDS;
    private static final int FLINK_SMALL_PARALLELISM = PARTITIONS;
    private static final int ROUNDS = 3;
    private static final long STATS_SETTLE_MS = 2_000L;
    private static final String DB_NAME = "ledger";

    private static final List<Scenario> SCENARIOS = List.of(
            new Scenario("rr1500", "round-robin, 1,500 accounts", 1_500, false),
            new Scenario("shuf1500", "shuffled, 1,500 accounts", 1_500, true),
            new Scenario("shuf30", "shuffled, 30 accounts (hot keys)", 30, true));

    /** The three contenders, in the order they run in round 0. */
    private static final List<String> APPROACHES = List.of("actor", "flink-default", "flink-p3");

    @Container
    @SuppressWarnings("resource")
    static final org.testcontainers.postgresql.PostgreSQLContainer POSTGRES =
            new org.testcontainers.postgresql.PostgreSQLContainer("postgres:16-alpine")
                    .withDatabaseName(DB_NAME)
                    .withUsername("bench")
                    .withPassword("bench")
                    .withCommand("postgres", "-c", "max_connections=200", "-c", "fsync=on");

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    record Scenario(String id, String description, int accounts, boolean shuffled) {
        String topic() {
            return id + "-ledger";
        }
    }

    record Run(String approach, int round, long wallMs, long steadyMs, int writers, PgStats db) {
    }

    @Test
    @DisplayName("every approach persists every change exactly once, in offset order; throughput and DB cost are reported")
    void benchmark_flink_vs_sharded_actor() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        long expectedTotal = MESSAGES;

        Map<Scenario, List<Run>> results = new LinkedHashMap<>();
        try (HikariDataSource dataSource = newPool()) {
            for (Scenario scenario : SCENARIOS) {
                LedgerTopic.create(bootstrap, scenario.topic(), PARTITIONS);
                try (BenchmarkProducer producer = new BenchmarkProducer(bootstrap)) {
                    assertEquals(expectedTotal, producer.produce(scenario.topic(), MESSAGES,
                            scenario.accounts(), scenario.shuffled()));
                }
                List<Long> partitionSizes = LedgerTopic.partitionSizes(bootstrap, scenario.topic(), PARTITIONS);
                List<Run> runs = new ArrayList<>();
                for (int round = 0; round < ROUNDS; round++) {
                    for (int i = 0; i < APPROACHES.size(); i++) {
                        String approach = APPROACHES.get((i + round) % APPROACHES.size());
                        runs.add(runOne(approach, scenario, bootstrap, dataSource, round, expectedTotal));
                    }
                }
                report(scenario, expectedTotal, partitionSizes, runs);
                results.put(scenario, runs);
            }
        }
        reportSummary(expectedTotal, results);
    }

    private Run runOne(String approach, Scenario scenario, String bootstrap, HikariDataSource dataSource,
                       int round, long expectedTotal) throws Exception {
        String prefix = approach.startsWith("flink") ? "flink" : "actor";
        PgLedgerStore store = new PgLedgerStore(dataSource, prefix);
        store.createSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        String group = scenario.id() + "-" + approach + "-" + round;
        PgStats before = settledStats(jdbc);

        FlinkLedgerJob.Result result = switch (approach) {
            case "actor" -> {
                PgActorLedgerOperations ops = new PgActorLedgerOperations(dataSource, store);
                try (ShardedLedgerRunner runner = new ShardedLedgerRunner(
                        bootstrap, scenario.topic(), PARTITIONS, group, ops, BATCH_SIZE)) {
                    yield runner.runUntil(expectedTotal);
                }
            }
            case "flink-default" -> FlinkLedgerJob.run(bootstrap, List.of(scenario.topic()), group,
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), prefix, BATCH_SIZE, null);
            case "flink-p3" -> FlinkLedgerJob.run(bootstrap, List.of(scenario.topic()), group,
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), prefix, BATCH_SIZE,
                    FLINK_SMALL_PARALLELISM);
            default -> throw new IllegalArgumentException(approach);
        };
        PgStats delta = settledStats(jdbc).minus(before);

        long perAccount = MESSAGES / scenario.accounts();
        assertEquals(expectedTotal, store.totalChanges(), approach + ": every change persisted exactly once");
        assertEquals(expectedTotal, store.totalBalance(), approach + ": total balance == message count");
        assertEquals(scenario.accounts(), store.distinctAccounts(),
                approach + ": every account touched");
        assertEquals(0, store.accountsWithBalanceOtherThan(perAccount),
                approach + ": every account's final balance == its event count");
        assertEquals(0, store.outOfOrderChanges(), approach + ": changes applied in Kafka offset order per account");
        return new Run(approach, round, result.wallMillis(), result.steadyMillis(), result.parallelism(), delta);
    }

    private static PgStats settledStats(JdbcTemplate jdbc) throws InterruptedException {
        Thread.sleep(STATS_SETTLE_MS);
        return PgStats.capture(jdbc, DB_NAME);
    }

    private static List<Run> of(List<Run> runs, String approach) {
        return runs.stream().filter(r -> r.approach().equals(approach)).toList();
    }

    private static long median(List<Run> runs, ToLongFunction<Run> metric) {
        long[] values = runs.stream().mapToLong(metric).sorted().toArray();
        return values[values.length / 2];
    }

    private static void report(Scenario scenario, long total, List<Long> partitionSizes, List<Run> runs) {
        System.out.println();
        System.out.println("==============================================================================================================");
        System.out.printf("FLINK VS SHARDED ACTOR [%s]  —  %d messages, 1 topic x %d partitions %s, %d rounds, poll %d, batch %d%n",
                scenario.description(), total, PARTITIONS, partitionSizes, ROUNDS,
                BenchmarkConsumers.MAX_POLL_RECORDS, BATCH_SIZE);
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-6s %-14s %8s %10s %10s %11s %11s %13s %14s%n", "round", "approach", "writers",
                "wall ms", "steady ms", "steady/s", "xact_commit", "wal_records", "wal_bytes");
        for (Run r : runs) {
            System.out.printf("%-6d %-14s %8d %10d %10d %11.0f %11d %13d %14d%n", r.round(), r.approach(), r.writers(),
                    r.wallMs(), r.steadyMs(), total / (Math.max(1, r.steadyMs()) / 1000.0),
                    r.db().xactCommit(), r.db().walRecords(), r.db().walBytes());
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-14s %10s %10s %11s %11s %14s%n", "MEDIAN", "wall ms", "steady ms", "steady/s",
                "xact_commit", "wal_bytes");
        for (String a : APPROACHES) {
            List<Run> rs = of(runs, a);
            long steady = median(rs, Run::steadyMs);
            System.out.printf("%-14s %10d %10d %11.0f %11d %14d%n", a, median(rs, Run::wallMs), steady,
                    total / (Math.max(1, steady) / 1000.0), median(rs, r -> r.db().xactCommit()),
                    median(rs, r -> r.db().walBytes()));
        }
        System.out.println("==============================================================================================================");
    }

    private static void reportSummary(long total, Map<Scenario, List<Run>> results) {
        System.out.println();
        System.out.println("==============================================================================================================");
        System.out.printf("FLINK VS SHARDED ACTOR SUMMARY  —  %d messages per scenario, medians of %d rounds (steady ms / wall ms)%n",
                total, ROUNDS);
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-34s %20s %20s %20s%n", "scenario", "actor", "flink-default", "flink-p3");
        results.forEach((scenario, runs) -> System.out.printf("%-34s %20s %20s %20s%n", scenario.description(),
                cell(of(runs, "actor")), cell(of(runs, "flink-default")), cell(of(runs, "flink-p3"))));
        System.out.println("==============================================================================================================");
        System.out.println();
    }

    private static String cell(List<Run> runs) {
        return median(runs, Run::steadyMs) + " / " + median(runs, Run::wallMs);
    }

    private HikariDataSource newPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(24);
        config.setMinimumIdle(4);
        config.setPoolName("bench-pg-pool");
        return new HikariDataSource(config);
    }

}
