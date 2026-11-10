package io.bufferfly.spring.benchmark;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 🦋 <b>Benchmark</b>: BufferFly actor (natural batching) vs. a <b>batching,
 * lock-based</b> Kafka consumer — both keep balances in memory and write to
 * Postgres in bulk, so this measures the actor against a fair conventional
 * design rather than one transaction per message.
 *
 * <h2>What is compared</h2>
 * <table>
 *   <tr><th>&nbsp;</th><th>Locking batch consumer</th><th>BufferFly actor</th></tr>
 *   <tr><td>Consumers</td><td>1 per topic + a {@code 7}-thread pool each</td>
 *       <td>1 per topic, routing to one actor per (topic,partition)</td></tr>
 *   <tr><td>Balance calc</td><td>in memory, per-account {@link java.util.concurrent.locks.ReentrantLock}</td>
 *       <td>in memory, single-writer actor, no locks</td></tr>
 *   <tr><td>DB writes</td><td>one synchronous bulk transaction per poll,
 *       blocking the next poll</td>
 *       <td>one bulk transaction per natural batch, asynchronous</td></tr>
 * </table>
 * Both poll {@link BenchmarkConsumers#MAX_POLL_RECORDS} records at a time, the
 * actor batches up to the same number, and both issue the same change insert and
 * balance upsert as JDBC batches through the same {@link PgLedgerStore} and pool.
 *
 * <h2>Scenarios (key distribution)</h2>
 * <ul>
 *   <li><b>round-robin, 500 accounts</b> — each account appears at most once in
 *       any 500 consecutive records, so the batch consumer's workers never touch
 *       the same account within a poll: no lock contention.</li>
 *   <li><b>shuffled, 500 accounts</b> — same counts, random order: an account
 *       can appear a few times in one poll.</li>
 *   <li><b>shuffled, 10 accounts</b> — about 50 records per account per poll,
 *       spread over 7 workers: heavy contention on few hot keys.</li>
 * </ul>
 * Every account receives exactly {@code MESSAGES_PER_TOPIC / accounts} credits
 * in every scenario, so final balances are asserted exactly.
 *
 * <h2>Fairness</h2>
 * <ul>
 *   <li>Each scenario's workload is produced once to its own topics; each run
 *       reads it from the beginning with its own consumer group and recreated
 *       tables.</li>
 *   <li>{@link #ROUNDS} rounds per scenario alternate which approach runs first;
 *       medians are reported.</li>
 *   <li>Completion is detected in memory after commit, never by querying the
 *       database, so the measured {@code xact_commit} is only the ingest work.</li>
 *   <li>Postgres flushes backend statistics asynchronously, so stats are
 *       captured after a {@link #STATS_SETTLE_MS} pause.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(2400)
@DisplayName("🦋 Benchmark: actor natural-batching vs locking batch consumer over Postgres + Kafka")
class PostgresKafkaBatchLedgerBenchmarkTest {

    private static final List<String> BASE_TOPICS = List.of("ledger-a", "ledger-b", "ledger-c");
    // 600k per scenario: long enough per run that consumer-group join and JIT
    // warmup do not dominate the measured time
    private static final int MESSAGES_PER_TOPIC = 200_000;
    // Actor persistence batch size; defaults to the poll size. Override with
    // -Dbufferfly.bench.actorBatchSize=N to compare batch sizes.
    private static final int ACTOR_BATCH_SIZE =
            Integer.getInteger("bufferfly.bench.actorBatchSize", BenchmarkConsumers.MAX_POLL_RECORDS);
    private static final int ROUNDS = 5;
    private static final long STATS_SETTLE_MS = 2_000L;
    private static final String DB_NAME = "ledger";
    private static final int DB_POOL_SIZE = 24;

    private static final List<Scenario> SCENARIOS = List.of(
            new Scenario("rr500", "round-robin, 500 accounts/topic", 500, false),
            new Scenario("shuf500", "shuffled, 500 accounts/topic", 500, true),
            new Scenario("shuf10", "shuffled, 10 accounts/topic", 10, true));

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

    /** A key distribution to benchmark, with its own topics. */
    record Scenario(String id, String description, int accountsPerTopic, boolean shuffled) {
        List<String> topics() {
            return BASE_TOPICS.stream().map(t -> id + "-" + t).toList();
        }
    }

    /**
     * One measured run of one approach. {@code contendedLocks} is {@code -1} for
     * the actor, which takes no locks. {@code phaseMs} holds time per phase summed
     * over the run's threads (one poll thread, or one persistence thread, per
     * topic), in reporting order.
     */
    record Run(String approach, int round, long millis, PgStats db, long contendedLocks, long outOfOrder,
               Map<String, Long> phaseMs) {
    }

    /** Medians of one scenario, for the cross-scenario summary. */
    record Summary(Scenario scenario, long lockBatchMs, long actorMs,
                   long lockBatchContended, long lockBatchOutOfOrder) {
    }

    @Test
    @DisplayName("both approaches persist every message exactly once in every key distribution; throughput, DB cost and lock contention are reported")
    void benchmark_actor_vs_locking_batch() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        long expectedTotal = (long) MESSAGES_PER_TOPIC * BASE_TOPICS.size();

        List<Summary> summaries = new ArrayList<>();
        try (HikariDataSource dataSource = newPool()) {
            for (Scenario scenario : SCENARIOS) {
                createTopics(bootstrap, scenario.topics());
                try (BenchmarkProducer producer = new BenchmarkProducer(bootstrap)) {
                    assertEquals(expectedTotal, producer.produce(
                            scenario.topics(), MESSAGES_PER_TOPIC, scenario.accountsPerTopic(), scenario.shuffled()));
                }
                summaries.add(runScenario(scenario, bootstrap, dataSource, expectedTotal));
            }
        }
        reportSummary(expectedTotal, summaries);
    }

    private Summary runScenario(Scenario scenario, String bootstrap, HikariDataSource dataSource,
                                long expectedTotal) throws Exception {
        List<Run> lockBatch = new ArrayList<>();
        List<Run> actor = new ArrayList<>();
        for (int round = 0; round < ROUNDS; round++) {
            if (round % 2 == 0) {
                lockBatch.add(runLockBatch(scenario, bootstrap, dataSource, round, expectedTotal));
                actor.add(runActor(scenario, bootstrap, dataSource, round, expectedTotal));
            } else {
                actor.add(runActor(scenario, bootstrap, dataSource, round, expectedTotal));
                lockBatch.add(runLockBatch(scenario, bootstrap, dataSource, round, expectedTotal));
            }
        }
        reportScenario(scenario, expectedTotal, lockBatch, actor);
        return new Summary(scenario,
                median(lockBatch, Run::millis), median(actor, Run::millis),
                median(lockBatch, Run::contendedLocks), median(lockBatch, Run::outOfOrder));
    }

    private Run runLockBatch(Scenario scenario, String bootstrap, HikariDataSource dataSource, int round,
                             long expectedTotal) throws Exception {
        PgLedgerStore store = new PgLedgerStore(dataSource, "lockbatch");
        store.createSchema();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        PgStats before = settledStats(jdbc);
        long ms;
        long contended;
        Map<String, Long> phases;
        try (LockingBatchLedgerConsumer consumer = new LockingBatchLedgerConsumer(
                bootstrap, scenario.topics(), scenario.id() + "-lockbatch-" + round, store)) {
            ms = consumer.runUntil(expectedTotal);
            contended = consumer.contendedLockAcquisitions();
            phases = phases("process (poll thread waits)", consumer.processMillis(),
                    "db transaction (blocks poll)", consumer.dbMillis(),
                    "kafka commitSync (blocks poll)", consumer.kafkaCommitMillis());
        }
        PgStats delta = settledStats(jdbc).minus(before);
        assertCorrect("locking batch", store, expectedTotal, scenario.accountsPerTopic());
        return new Run("lockbatch", round, ms, delta, contended, store.outOfOrderChanges(), phases);
    }

    private Run runActor(Scenario scenario, String bootstrap, HikariDataSource dataSource, int round,
                         long expectedTotal) throws Exception {
        PgLedgerStore store = new PgLedgerStore(dataSource, "actor");
        store.createSchema();
        PgActorLedgerOperations ops = new PgActorLedgerOperations(dataSource, store);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        PgStats before = settledStats(jdbc);
        long ms;
        Map<String, Long> phases;
        try (ActorLedgerConsumer consumer = new ActorLedgerConsumer(
                bootstrap, scenario.topics(), scenario.id() + "-actor-" + round, ops,
                ACTOR_BATCH_SIZE)) {
            ms = consumer.runUntil(expectedTotal);
            phases = phases("poll thread: JSON parse", consumer.parseMillis(),
                    "poll thread: tell (mailbox)", consumer.tellMillis(),
                    "persistence thread: db tx", ops.dbMillis());
        }
        PgStats delta = settledStats(jdbc).minus(before);
        assertCorrect("actor", store, expectedTotal, scenario.accountsPerTopic());
        int outOfOrder = store.outOfOrderChanges();
        assertEquals(0, outOfOrder, "actor: a single writer per partition applies changes in offset order");
        return new Run("actor", round, ms, delta, -1, outOfOrder, phases);
    }

    private static Map<String, Long> phases(String n1, long v1, String n2, long v2, String n3, long v3) {
        Map<String, Long> m = new java.util.LinkedHashMap<>();
        m.put(n1, v1);
        m.put(n2, v2);
        m.put(n3, v3);
        return m;
    }

    /** Every message persisted exactly once, and every account holds its exact final balance. */
    private static void assertCorrect(String approach, PgLedgerStore store, long expectedTotal, int accountsPerTopic) {
        long perAccount = MESSAGES_PER_TOPIC / accountsPerTopic;
        assertEquals(expectedTotal, store.totalChanges(), approach + ": every message persisted exactly once");
        assertEquals(expectedTotal, store.totalBalance(), approach + ": total balance == message count");
        assertEquals((long) accountsPerTopic * BASE_TOPICS.size(), store.distinctAccounts(),
                approach + ": every account touched");
        assertEquals(0, store.accountsWithBalanceOtherThan(perAccount),
                approach + ": every account's final balance == its event count");
    }

    private static PgStats settledStats(JdbcTemplate jdbc) throws InterruptedException {
        Thread.sleep(STATS_SETTLE_MS);
        return PgStats.capture(jdbc, DB_NAME);
    }

    private static void reportScenario(Scenario scenario, long total, List<Run> lockBatch, List<Run> actor) {
        System.out.println();
        System.out.println("==============================================================================================================");
        System.out.printf("BATCH LEDGER BENCHMARK [%s]  —  %d messages over %d topics, %d rounds, poll %d, actor batch %d%n",
                scenario.description(), total, BASE_TOPICS.size(), ROUNDS,
                BenchmarkConsumers.MAX_POLL_RECORDS, ACTOR_BATCH_SIZE);
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-6s %-10s %9s %9s %11s %12s %13s %15s %13s%n", "round", "approach", "time ms", "msg/s",
                "xact_commit", "wal_records", "wal_bytes", "contended_locks", "out_of_order");
        List<Run> all = new ArrayList<>(lockBatch);
        all.addAll(actor);
        all.sort(Comparator.comparingInt(Run::round).thenComparing(Run::approach));
        for (Run r : all) {
            System.out.printf("%-6d %-10s %9d %9.0f %11d %12d %13d %15s %13d%n", r.round(), r.approach(), r.millis(),
                    total / (r.millis() / 1000.0), r.db().xactCommit(), r.db().walRecords(), r.db().walBytes(),
                    r.contendedLocks() < 0 ? "n/a" : Long.toString(r.contendedLocks()), r.outOfOrder());
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        long lbMs = median(lockBatch, Run::millis);
        long acMs = median(actor, Run::millis);
        long lbCommits = median(lockBatch, r -> r.db().xactCommit());
        long acCommits = median(actor, r -> r.db().xactCommit());
        System.out.printf("%-30s %20s %20s%n", "MEDIAN", "LOCKING BATCH", "ACTOR (batching)");
        System.out.printf("%-30s %17d ms %17d ms%n", "end-to-end time", lbMs, acMs);
        System.out.printf("%-30s %18.0f/s %18.0f/s%n", "throughput", total / (lbMs / 1000.0), total / (acMs / 1000.0));
        System.out.printf("%-30s %20d %20d%n", "pg xact_commit (delta)", lbCommits, acCommits);
        System.out.printf("%-30s %20d %20d%n", "pg wal_records (delta)",
                median(lockBatch, r -> r.db().walRecords()), median(actor, r -> r.db().walRecords()));
        System.out.printf("%-30s %20d %20d%n", "pg wal_bytes (delta)",
                median(lockBatch, r -> r.db().walBytes()), median(actor, r -> r.db().walBytes()));
        System.out.printf("%-30s %20d %20s%n", "contended lock acquisitions",
                median(lockBatch, Run::contendedLocks), "n/a");
        System.out.printf("%-30s %20d %20d%n", "out-of-order changes",
                median(lockBatch, Run::outOfOrder), median(actor, Run::outOfOrder));
        System.out.printf("Actor vs locking batch: time %.2fx  (>1 = actor faster)%n", lbMs / (double) Math.max(1, acMs));
        System.out.println("-- where the time goes (median, per thread = sum / " + BASE_TOPICS.size()
                + " topics, % of median wall time) --");
        printPhases("LOCKING BATCH", lockBatch, lbMs);
        printPhases("ACTOR", actor, acMs);
        System.out.println("==============================================================================================================");
    }

    private static void printPhases(String label, List<Run> runs, long wallMs) {
        for (String phase : runs.get(0).phaseMs().keySet()) {
            long perThread = median(runs, r -> r.phaseMs().get(phase)) / BASE_TOPICS.size();
            System.out.printf("  %-14s %-32s %9d ms %6.1f%%%n", label, phase, perThread,
                    100.0 * perThread / Math.max(1, wallMs));
        }
    }

    private static void reportSummary(long total, List<Summary> summaries) {
        System.out.println();
        System.out.println("==============================================================================================================");
        System.out.printf("BATCH LEDGER BENCHMARK SUMMARY  —  %d messages per scenario, medians of %d rounds%n", total, ROUNDS);
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-34s %14s %14s %10s %18s %14s%n", "scenario", "lockbatch ms", "actor ms", "actor x",
                "contended locks", "out of order");
        for (Summary s : summaries) {
            System.out.printf("%-34s %14d %14d %9.2fx %18d %14d%n", s.scenario().description(),
                    s.lockBatchMs(), s.actorMs(), s.lockBatchMs() / (double) Math.max(1, s.actorMs()),
                    s.lockBatchContended(), s.lockBatchOutOfOrder());
        }
        System.out.println("==============================================================================================================");
        System.out.println();
    }

    private static long median(List<Run> runs, ToLongFunction<Run> metric) {
        long[] values = runs.stream().mapToLong(metric).sorted().toArray();
        return values[values.length / 2];
    }

    private HikariDataSource newPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(DB_POOL_SIZE);
        config.setMinimumIdle(DB_POOL_SIZE);
        config.setPoolName("bench-pg-pool");
        config.setAutoCommit(true);
        return new HikariDataSource(config);
    }

    private void createTopics(String bootstrap, List<String> topics) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            // one partition per topic: one actor (single writer) per topic on the
            // actor side, one consumer per topic on both sides
            admin.createTopics(topics.stream().map(t -> new NewTopic(t, 1, (short) 1)).toList()).all().get();
        }
    }
}
