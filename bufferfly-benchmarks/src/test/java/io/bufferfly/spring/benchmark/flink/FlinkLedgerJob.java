package io.bufferfly.spring.benchmark.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.bufferfly.spring.benchmark.BenchmarkConsumers;
import io.bufferfly.spring.benchmark.PgLedgerChange;
import io.bufferfly.spring.ledger.AddToAccount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The <b>Apache Flink</b> version of the benchmark ledger, written the standard
 * Flink way:
 * <ol>
 *   <li>a bounded {@link KafkaSource} reads the topics from the earliest to the
 *       latest offset, polling {@link BenchmarkConsumers#MAX_POLL_RECORDS}
 *       records at a time;</li>
 *   <li>{@code keyBy(account)} partitions the stream by account;</li>
 *   <li>a {@link KeyedProcessFunction} keeps each account's balance in
 *       <b>keyed state</b> ({@link ValueState}, held in memory by Flink's default
 *       heap state backend) and emits one change per event;</li>
 *   <li>the JDBC connector's {@link JdbcSink} (at-least-once) writes the changes
 *       in JDBC batches of {@code batchSize}: one statement per change that
 *       inserts the change row and upserts the account's balance.</li>
 * </ol>
 * Everything else is left at Flink's defaults: checkpointing off, local
 * execution environment, default parallelism unless one is passed in.
 *
 * <p>All functions are static nested classes: Flink's closure cleaner cannot
 * read Java 25 class files of anonymous or inner classes.
 */
public final class FlinkLedgerJob {

    /** Time the first event reached the keyed function (nanoTime), for steady-state timing. */
    static final AtomicLong FIRST_EVENT_NANOS = new AtomicLong();

    private FlinkLedgerJob() {
    }

    /** One consumed credit with its Kafka coordinates. */
    public record LedgerEvent(String topic, int partition, long offset, String account, long amount) {
    }

    /** Result of one job execution. */
    public record Result(long wallMillis, long steadyMillis, int parallelism) {
    }

    /**
     * Builds and runs the job to completion (it ends when the bounded source has
     * read every topic up to the offsets that existed at start).
     *
     * @param parallelism job parallelism, or {@code null} for Flink's default
     */
    public static Result run(String bootstrap, List<String> topics, String groupId,
                             String jdbcUrl, String user, String password,
                             String tablePrefix, int batchSize, Integer parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (parallelism != null) {
            env.setParallelism(parallelism);
        }

        KafkaSource<LedgerEvent> source = KafkaSource.<LedgerEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        Integer.toString(BenchmarkConsumers.MAX_POLL_RECORDS))
                .setDeserializer(new LedgerEventDeserializer())
                .build();

        JdbcSink<PgLedgerChange> sink = JdbcSink.<PgLedgerChange>builder()
                .withQueryStatement(changeSql(tablePrefix), new ChangeStatement())
                .withExecutionOptions(JdbcExecutionOptions.builder().withBatchSize(batchSize).build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(user)
                        .withPassword(password)
                        .build());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-ledger")
                .keyBy(new AccountKey())
                .process(new BalanceFunction())
                .sinkTo(sink);

        FIRST_EVENT_NANOS.set(0L);
        long start = System.nanoTime();
        env.execute("flink-ledger-" + groupId);
        long end = System.nanoTime();
        return new Result((end - start) / 1_000_000L,
                (end - FIRST_EVENT_NANOS.get()) / 1_000_000L, env.getParallelism());
    }

    /**
     * One statement per change: insert the change row (idempotent by
     * {@code (topic, partition, offset)}) and, only if it was new, upsert the
     * account's balance to the change's resulting balance.
     */
    static String changeSql(String prefix) {
        return """
                WITH ins AS (
                    INSERT INTO %1$s_account_change
                        (account_number, amount, resulting_balance, topic, partition, offset_value)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (topic, partition, offset_value) DO NOTHING
                    RETURNING account_number, resulting_balance)
                INSERT INTO %1$s_account_balance (account_number, balance)
                SELECT account_number, resulting_balance FROM ins
                ON CONFLICT (account_number) DO UPDATE SET balance = EXCLUDED.balance
                """.formatted(prefix);
    }

    /** Parses the JSON credit and keeps the Kafka coordinates. */
    static final class LedgerEventDeserializer implements KafkaRecordDeserializationSchema<LedgerEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<LedgerEvent> out) throws IOException {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            AddToAccount cmd = mapper.readValue(record.value(), AddToAccount.class);
            out.collect(new LedgerEvent(record.topic(), record.partition(), record.offset(),
                    cmd.accountNumber(), cmd.amount()));
        }

        @Override
        public TypeInformation<LedgerEvent> getProducedType() {
            return TypeInformation.of(LedgerEvent.class);
        }
    }

    static final class AccountKey implements KeySelector<LedgerEvent, String> {
        @Override
        public String getKey(LedgerEvent event) {
            return event.account();
        }
    }

    /** Keeps each account's running balance in keyed {@link ValueState}. */
    static final class BalanceFunction extends KeyedProcessFunction<String, LedgerEvent, PgLedgerChange> {
        private transient ValueState<Long> balance;

        @Override
        public void open(OpenContext openContext) {
            balance = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Types.LONG));
        }

        @Override
        public void processElement(LedgerEvent event, Context ctx, Collector<PgLedgerChange> out) throws Exception {
            FIRST_EVENT_NANOS.compareAndSet(0L, System.nanoTime());
            Long current = balance.value();
            long next = (current == null ? 0L : current) + event.amount();
            balance.update(next);
            out.collect(new PgLedgerChange(event.topic(), event.partition(), event.offset(),
                    event.account(), event.amount(), next));
        }
    }

    static final class ChangeStatement implements JdbcStatementBuilder<PgLedgerChange> {
        @Override
        public void accept(PreparedStatement ps, PgLedgerChange c) throws SQLException {
            ps.setString(1, c.accountNumber());
            ps.setLong(2, c.amount());
            ps.setLong(3, c.resultingBalance());
            ps.setString(4, c.topic());
            ps.setInt(5, c.partition());
            ps.setLong(6, c.offset());
        }
    }
}
