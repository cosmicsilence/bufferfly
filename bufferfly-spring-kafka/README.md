# ⚡ bufferfly-spring-kafka

Spring Boot auto-configuration that turns a plain class into a **sharded,
persistent, backpressure-aware Kafka consumer** backed by a BufferFly actor.

You write the actor and the bulk-write logic; BufferFly wires the Kafka
consumer, the per-partition lifecycle, the natural-batching persistence runner,
the backpressure that pauses partitions, and the crash-safe restart-and-rewind
recovery.

It is the "batteries-included" front door to the [bufferfly-core](../bufferfly-core)
actor engine for the most common high-volume ingestion shape: **Kafka → actor
(in-memory shock absorber) → one bulk write per batch → relational database.**

---

## Table of contents

- [Why this module exists](#why-this-module-exists)
- [Setup](#setup)
- [Usage: the `@ShardedActor` annotation](#usage-the-shardedactor-annotation)
- [Implementing the bulk write: `TransactionalOperations`](#implementing-the-bulk-write-transactionaloperations)
- [What you get, wired for free](#what-you-get-wired-for-free)
- [🧪 Poison pills: classify, dead-letter, halt & rewind](#-poison-pills-classify-dead-letter-halt--rewind)
  - [What is a poison pill](#what-is-a-poison-pill)
  - [How you classify one](#how-you-classify-one)
  - [What the runner does with it](#what-the-runner-does-with-it)
  - [Why halt instead of skipping ahead](#why-halt-instead-of-skipping-ahead)
  - [Dead-lettering and replay](#dead-lettering-and-replay)
  - [A complete example](#a-complete-example)
  - [What to watch (metrics)](#what-to-watch-metrics)
- [🩺 Health check](#-health-check)
- [📊 Benchmark: actor natural-batching vs classic locking consumer](#-benchmark-actor-natural-batching-vs-classic-locking-consumer)
  - [How the benchmark is built](#how-the-benchmark-is-built)
  - [What the classic consumer does](#what-the-classic-consumer-does)
  - [What the actor consumer does](#what-the-actor-consumer-does)
  - [Results](#results)
  - [Why the actor is so much faster here](#why-the-actor-is-so-much-faster-here)
- [📊 Benchmark: actor natural-batching vs a batching locking consumer](#-benchmark-actor-natural-batching-vs-a-batching-locking-consumer)
  - [How the two are configured](#how-the-two-are-configured)
  - [Key distributions](#key-distributions)
  - [Results](#results-1)
  - [What this shows](#what-this-shows)
  - [Not measured yet](#not-measured-yet)
- [📊 Benchmark: sharded actors vs Apache Flink](#-benchmark-sharded-actors-vs-apache-flink-keyed-state--jdbc-sink)
  - [How the two are built](#how-the-two-are-built)
  - [Results](#results-2)
  - [What this shows](#what-this-shows-1)
- [📊 Benchmark: `ShardedActorContainer` throughput](#-benchmark-shardedactorcontainer-throughput-kafka--actors-no-database)
  - [Run them yourself](#run-them-yourself)

---

## Why this module exists

A very common ingestion pipeline looks like this:

```
Kafka partition ──► consumer ──► for each record: BEGIN; INSERT; COMMIT ──► Postgres
```

Under a burst it falls apart: one tiny transaction per message means the database
spends most of its time on transaction overhead (begin/commit, WAL flush, index
maintenance) rather than on your data; adding consumer threads just drains the
connection pool; and there is no real backpressure, so work piles up in the JVM
heap until something falls over.

BufferFly puts a thin in-memory actor between Kafka and the database. The actor
transforms each message (pure, single-threaded, no locks, no I/O) and the
persistence runner **coalesces many messages into a single bulk transaction**.
Millions of tiny commits become thousands of fat ones; bursts are absorbed in
memory and smoothed into a steady write rate; and when the buffer saturates, the
Kafka partition is paused instead of the heap growing without bound.

This module does all that wiring for you from a single annotation.

---

## Setup

Spring Boot's Kafka auto-configuration provides the `ConsumerFactory` from your
`spring.kafka.*` properties; this module builds on it.

```groovy
implementation 'io.github.cosmicsilence:bufferfly-spring-kafka'
```

Requires a **Java 25** toolchain.

---

## Usage: the `@ShardedActor` annotation

Annotate a factory bean with `@ShardedActor` and implement `ShardedActorFactory`.
BufferFly's `ShardedActorBeanPostProcessor` discovers the annotated bean and
stands up a `ConcurrentMessageListenerContainer` for the topic.

```java
@Component
@ShardedActor(
        name          = "ledger",
        topic         = "ledger-events",
        concurrency   = 3,        // Kafka listener concurrency (consumers in the group)
        highWatermark = 50_000,   // engage backpressure at this buffer depth
        lowWatermark  = 20_000)   // release backpressure here (hysteresis)
public class LedgerShardFactory implements ShardedActorFactory<AddToAccount> {

    private final LedgerOperations operations; // your TransactionalOperations (the bulk write)

    public LedgerShardFactory(LedgerOperations operations) {
        this.operations = operations;
    }

    @Override
    public Actor<ConsumedRecord<AddToAccount>> createActor(TopicPartition tp, KafkaContext ctx) {
        // one actor is created per assigned partition
        PersistenceConfig<LedgerChange> config = PersistenceConfig.builder(operations)
                .batchSize(200)
                .maxRetries(5)
                .onUnrecoverable(ctx::restartActor) // halt + restart + rewind on fatal failure
                .build();
        return new LedgerActor(actorName(tp), ctx, operations, config);
    }
}
```

The actor itself splits the work cleanly between a **pure, in-memory transform**
(`apply`, which runs on the single actor thread) and a **bulk write**
(`TransactionalOperations`, which runs once per natural batch):

```java
public final class LedgerActor extends AbstractPersistentActor<ConsumedRecord<AddToAccount>, LedgerChange> {

    @Override
    public PersistenceRunner.PersistenceEvent<LedgerChange> apply(ConsumedRecord<AddToAccount> record) {
        // pure, single-threaded, no I/O — just transform and project in memory
        long newBalance = balances.merge(record.value().accountNumber(), record.value().amount(), Long::sum);
        return PersistenceRunner.newEvent(new LedgerChange(/* ... */));
    }
}
```

See `src/test/java/io/bufferfly/spring/ledger/` for a complete, runnable showcase
on embedded Kafka + HSQLDB.

### Wiring it with Spring configuration

Everything is ordinary Spring. You provide a `@Configuration` that declares your
collaborator beans (the `DataSource`, your `TransactionalOperations`, any read
models) and the Kafka `ConsumerFactory`; Spring injects those into the
`@ShardedActor` factory, and BufferFly's auto-configuration (`ActorManager` +
`ShardedActorBeanPostProcessor`) does the rest — one bean per annotated factory,
one actor per partition.

```java
@Configuration
@Import(BufferflySpringAutoConfiguration.class)   // ActorManager + the @ShardedActor post-processor
@ComponentScan("com.acme.ledger")                 // picks up the @Component @ShardedActor factory
public class LedgerConfig {

    @Bean
    DataSource dataSource(DataSourceProperties props) {
        return props.initializeDataSourceBuilder().build();
    }

    /** Your bulk-write + fencing logic — injected into the @ShardedActor factory. */
    @Bean
    LedgerOperations ledgerOperations(DataSource dataSource) {
        return new LedgerOperations(dataSource);
    }

    /** Any collaborator the actor needs (read model, notifier, ...). */
    @Bean
    BalanceConfirmationService balanceConfirmationService() {
        return new BalanceConfirmationService();
    }

    /**
     * The Kafka ConsumerFactory the post-processor injects to build consumers.
     * In a plain Spring Boot app this bean is provided automatically from your
     * spring.kafka.* properties — declare it explicitly only to customize it
     * (e.g. a specific value deserializer).
     */
    @Bean
    ConsumerFactory<Object, Object> consumerFactory(KafkaProperties kafka) {
        var value = new JsonDeserializer<>(AddToAccount.class);
        value.addTrustedPackages("com.acme.ledger");
        return new DefaultKafkaConsumerFactory<>(
                kafka.buildConsumerProperties(null), new StringDeserializer(), value);
    }
}
```

With that in place the `@ShardedActor` factory bean shown above has its
`LedgerOperations` and `BalanceConfirmationService` injected by Spring, and the
post-processor stands up the sharded consumer at startup and tears it down on
context shutdown. (In a Spring Boot application `BufferflySpringAutoConfiguration`
is picked up automatically, so the `@Import` is only needed outside Boot's
auto-configuration — e.g. in a plain `@SpringJUnitConfig` test; see
`LedgerShowcaseTest`.)

---

## Implementing the bulk write: `TransactionalOperations`

Your `@ShardedActor` factory hands the actor a `PersistenceConfig` built around a
`TransactionalOperations<E>` — **the one place you write the bulk write**, and
the one place you tell BufferFly how to react to a failure.

```java
public interface TransactionalOperations<E> {

    // Persist one natural batch atomically, advancing the per-partition fencing
    // token (the Kafka offset) in the SAME transaction. Signal failures by the
    // exception you throw (see below).
    long persistOnTransaction(List<E> batch, long currentToken);

    // Called once for the record that could not be persisted (the poison pill),
    // AFTER the good records before it have committed. Dead-letter it here.
    long onPoisonPill(E event, long currentToken, Exception cause);
}
```

**How you signal a failure is by the exception you throw** from
`persistOnTransaction`; the runner reacts differently to each:

| Throw | Runner reaction |
| :--- | :--- |
| `TransientException` | Retry the **whole batch** with exponential back-off, up to `maxRetries`. After that, treated as unrecoverable. |
| `SingleItemException` | The batch contains a **poison pill**. The runner re-submits items one-by-one to find it, dead-letters the offender via `onPoisonPill`, then **halts** (see below). |
| `UnrecoverableException` | Halt immediately and fire `onUnrecoverable` (e.g. a failed fencing CAS = a zombie actor). |
| any other exception | Treated as `UnrecoverableException`. |

You rarely write this classification by hand. The JDBC base class
`AbstractJdbcTransactionalOperations<E extends OffsetBearing>` already opens one
transaction per batch, advances the fencing offset with a compare-and-set, and
provides a `classify(RuntimeException)` that maps a Spring
`DataIntegrityViolationException` (e.g. a value too long for a column, a
constraint violation) to `SingleItemException` and everything else to
`UnrecoverableException`. You implement only `persistBatch(List<E>)` (the data
writes), `deadLetter(E, cause)` (where a poison pill goes), and — if your
database has transient/poison conditions the default does not recognise —
override `classify(...)`.

```java
public final class LedgerOperations extends AbstractJdbcTransactionalOperations<LedgerChange> {

    @Override
    protected void persistBatch(List<LedgerChange> batch) {
        // runs inside the base class's transaction, after the offset is fenced
        // + advanced. Just write the data (idempotently by topic/partition/offset).
    }

    @Override
    protected void deadLetter(LedgerChange event, Exception cause) {
        // the record that can never be persisted — insert it into a dead_message
        // table (or publish to a DLQ topic). Runs in the poison-pill transaction,
        // which also advances the offset PAST this record so a restart resumes after it.
    }

    // Optional: recognise DB-specific transient/poison conditions.
    // @Override protected RuntimeException classify(RuntimeException e) { ... }
}
```

---

## 🧪 Poison pills: classify, dead-letter, halt & rewind

A **poison pill** is a record your database will reject no matter how many times
you retry — it is bad *data*, not a transient outage. Left unhandled it would
wedge a partition forever (retry → fail → retry …). BufferFly isolates it,
dead-letters it so it is never silently lost, and recovers the partition
cleanly.

### What is a poison pill

Anything that causes a permanent, data-level write failure, for example:

- a field longer than its column allows (a `VARCHAR` overflow),
- a `NOT NULL` / `CHECK` / foreign-key constraint violation,
- a value the schema can never represent.

These surface from JDBC as a Spring `DataIntegrityViolationException`. By
contrast, a dropped connection or a lock timeout is **transient** — a different
exception, retried, not dead-lettered.

### How you classify one

You never pre-validate records to guess which are poison — **the database is the
source of truth.** You let the write fail and classify the exception:

- With `AbstractJdbcTransactionalOperations`, `classify(...)` already turns a
  `DataIntegrityViolationException` into `SingleItemException`. Nothing to do.
- With a hand-written `TransactionalOperations`, catch the data-violation and
  `throw new SingleItemException(...)`; for a transient error
  `throw new TransientException(...)`.

### What the runner does with it

A batch flows in as one bulk transaction. When that transaction fails with
`SingleItemException`, the runner switches to **item-by-item isolation** to find
the offender, then **halts**:

```
batch = [ good(100), good(101), POISON(102), good(103) ]   (one bulk tx -> fails)

  isolate item-by-item:
    persist good(100)   -> committed            (its own tx)
    persist good(101)   -> committed            (its own tx)
    persist POISON(102) -> fails  -> onPoisonPill(102): dead-letter + advance fence past 102
    HALT                -> good(103) is NOT persisted here

  fire onUnrecoverable -> ctx.restartActor():
    pause partition -> stop actor -> rewind to committed offset -> start fresh actor -> resume
    good(103) is re-consumed from Kafka and persisted cleanly
```

- Good records **before** the pill commit normally (each in its own transaction
  during isolation).
- The pill is handed to `onPoisonPill`, which dead-letters it and advances the
  stored fencing offset **past** it (so a rewind resumes *after* the pill, never
  re-delivering it in a loop).
- The runner then **stops** and fires the `onUnrecoverable` hook you wired to
  `ctx.restartActor()`. The container pauses the partition, stops the actor,
  rewinds to the last committed offset, starts a fresh actor, and resumes. Kafka
  re-delivers everything after the pill, which is processed cleanly.

### Why halt instead of skipping ahead

The actor already applied every record in the batch to its **in-memory** state
(that is what produced the events). Persistence, however, stopped at the pill. If
the runner kept persisting the records *after* the pill, the durable store would
contain writes the actor made after a record it never durably accepted — while
the actor's in-memory state reflects the whole batch. That split-brain surfaces
on the next rehydration, where the restarted actor replays from the committed
offset and re-derives a state that disagrees with what was written.

Halting makes the boundary clean: **everything up to and including the
dead-lettered pill is durable and fenced; everything after it is discarded and
re-processed deterministically** when the actor restarts and rewinds. No loss, no
duplicates, no divergence.

### Dead-lettering and replay

`onPoisonPill` is **where you decide what "dead-letter" means** — it must not
throw. Common choices: insert into a `dead_message(payload, topic, partition,
offset, cause)` table (as the showcase does), or publish to a dedicated DLQ
topic. Because the record carries its `(topic, partition, offset)`, you can later
inspect, fix, and replay it out-of-band. The idempotent sink (keyed by
`(topic, partition, offset)`) means a corrected replay never double-counts.

### A complete example

From the runnable showcase (`src/test/java/io/bufferfly/spring/ledger/`):

```java
// LedgerOperations — poison pill = a reason longer than the column allows.
public static final int REASON_MAX_LENGTH = 256;

@Override
protected void persistBatch(List<LedgerChange> batch) {
    // We do NOT pre-check length. If a reason is too long, HSQLDB rejects the
    // INSERT with a DataIntegrityViolationException, which the base class's
    // classify() turns into a SingleItemException — the DB is the source of truth.
    for (LedgerChange c : batch) {
        jdbc().update("INSERT INTO account_change (...) VALUES (...)", /* ... */);
    }
    // ... balance upserts ...
}

@Override
protected void deadLetter(LedgerChange event, Exception cause) {
    // Record it so it is never lost. Runs in the poison-pill transaction, which
    // also advances the offset past this record -> on restart, preStart rewinds
    // to storedOffset+1 and resumes AFTER the pill.
    jdbc().update("INSERT INTO dead_message (payload, topic, partition, offset_value, cause) VALUES (?,?,?,?,?)",
            describe(event), event.topic(), event.partition(), event.offset(), cause.getMessage());
}
```

`LedgerRestartRewindTest` drives this end-to-end over embedded Kafka: 5 good
messages, then a poison pill, then 4 good messages — all for one account. It
asserts that **all 9 good changes persist exactly once** (the final balance
excludes the pill), the **pill is dead-lettered exactly once**, and the actor
**restarted and rewound** — proving nothing before *or* after the pill is lost or
duplicated.

### What to watch (metrics)

When the optional `bufferfly-observability-otel` collector is wired, each step is
observable per component (the `topic-partition` actor name):

| Signal | Meaning |
| :--- | :--- |
| `bufferfly.persistence.poison_pills` | A record was dead-lettered. A rising rate means bad data is arriving — inspect the dead-letter sink. |
| `bufferfly.persistence.unrecoverable` | The runner halted (a poison pill always halts, so this moves with it). |
| `bufferfly.actor.restarted` | The supervisor restarted the actor after the halt. |
| `bufferfly.partition.paused` / `.resumed` | The partition was paused during the restart and resumed after the rewind. |

A healthy poison-pill event is a small, correlated bump across these four with
no sustained growth. A climbing `poison_pills` **and** `actor.restarted` together
is a crash-loop signal — a record the DB keeps rejecting that is not being
dead-lettered correctly.

---

## What you get, wired for free

| Capability | What BufferFly does |
| :--- | :--- |
| **One actor per partition** | On `onPartitionsAssigned` it builds a `KafkaContext`, asks your factory for an actor, and starts it; on revoke/loss it stops that actor. Per-partition ordering is preserved — one single-writer actor owns a partition. |
| **Records as `ConsumedRecord<V>`** | The deserialized value plus `topic`, `partition`, `offset`, so a persistent actor always has the offset for the fencing-token pattern. |
| **No message loss (per-partition commits)** | A batch listener (one call per `poll()`) commits with a non-blocking `commitAsync` only the partitions whose whole slice reached their actor; durability relies not on the Kafka commit but on storing the consumed **offset in the same DB transaction as the data**. On restart the actor seeks back to its DB-stored offset. |
| **Offset as a fencing token** | The stored offset is advanced with a compare-and-set (`WHERE latest_offset = :expected`) in the same transaction. A failed CAS means a zombie actor is racing the new owner → the batch is rejected as `UnrecoverableException`. |
| **Exactly-once sink** | Writes are idempotent by `(topic, partition, offset)` — a re-delivered record is skipped, never double-inserted. |
| **Backpressure that pauses the partition** | When the buffer crosses the high watermark, `KafkaContext.pausePartition()` stops fetching for that partition only; `resumePartition()` fires once it recedes to the low watermark. |
| **Restart + rewind on fatal failure** | `onUnrecoverable` → `ctx.restartActor()` stops the current actor and starts a fresh one that rehydrates in-memory state and seeks back to the last committed offset. |
| **Poison-pill dead-lettering** | A record the DB can never accept is classified as `SingleItemException`; the runner isolates the batch item-by-item, persists the good records, and dead-letters the offender. |
| **Post-commit side effects** | `PersistenceEvent`s can carry a `SideEffect` that runs only **after** the batch is durably committed — the right place to update a read model or emit a downstream event. |
| **Health check (opt-in)** | With Spring Boot Actuator on the classpath, a `bufferfly` health indicator reports **DOWN only when a listener container is not running** — the one condition a pod restart fixes. See [Health check](#-health-check). |

---

## 🩺 Health check

When **Spring Boot Actuator** is on the classpath, BufferFly auto-registers a
`HealthIndicator` named `bufferfly` that contributes to `/actuator/health`. It is
deliberately **minimal and stateless**:

```json
// GET /actuator/health   (UP)
{
  "status": "UP",
  "components": {
    "bufferfly": {
      "status": "UP",
      "details": { "ledger": "running", "containers": 1 }
    }
  }
}
```

```json
// a container whose Kafka listener is not running
{
  "status": "DOWN",
  "components": {
    "bufferfly": {
      "status": "DOWN",
      "details": { "ledger": "not running", "containers": 1 }
    }
  }
}
```

### What it checks — and why only this

It reports **DOWN only when a `@ShardedActor`'s Kafka listener container is not
running**, and **UP** otherwise. That is the single condition a **pod restart can
actually fix** (the listener thread died, or a wiring/startup bug left it down).

A health check drives orchestration — a liveness/readiness probe going DOWN
restarts the pod — so the only honest thing to report DOWN for is something a
restart resolves. BufferFly therefore **deliberately does not** fail health on:

| Condition | Why it is NOT a health failure |
| :--- | :--- |
| **A paused partition (backpressure)** | This is the shock absorber working *correctly* — Kafka is told to wait while the DB catches up. Restarting drops the in-memory buffer and re-saturates the DB on the replacement pod → a kill-loop that amplifies the incident. |
| **An actor crash-looping on a poison pill** | Handled in-process by the restart supervisor (backoff + dead-letter). A restart just moves the same bad-data loop to a fresh pod; it cannot fix the data. |
| **Rebalance churn** | Partitions momentarily assigned-but-starting or just-revoked are transient and self-healing. Reacting would make the probe flap on every deploy/scale. Because the indicator never inspects partition-level state, it is immune to rebalance staleness. |

Those conditions belong on **dashboards and alerts** (via the metrics below),
answering *"is the system behaving well?"* — not on a restart decision, which
answers *"should this pod exist?"*. See [What to watch (metrics)](#what-to-watch-metrics)
and the poison-pill metrics table above: `bufferfly.partition.paused`,
`bufferfly.poison_pills`, `bufferfly.actor.restarted`.

### Enabling it

It is **opt-in by dependency**: add Actuator and the health check appears; without
Actuator, BufferFly pulls in none of the health module and registers no bean.

```groovy
implementation 'org.springframework.boot:spring-boot-starter-actuator'
```

```yaml
# expose the health endpoint (standard Boot config)
management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: always   # to see the per-container details shown above
```

To override it, define your own bean named `bufferflyHealthIndicator` (the
auto-configuration backs off via `@ConditionalOnMissingBean`).

---

## 📊 Benchmark: actor natural-batching vs classic locking consumer

`PostgresKafkaLedgerBenchmarkTest` pits the BufferFly actor approach against the
classic consumer it is designed to replace — **both ingesting the exact same
Kafka workload into the same real Postgres**, on throwaway Docker containers via
Testcontainers. Same schema, same SQL, same DataSource; the only difference is
*how* each one drives the database.

### How the benchmark is built

- **Infrastructure (Testcontainers).** A real **Postgres 16** and a real
  **Apache Kafka** broker are started in Docker for the test. The class is
  annotated `@Testcontainers(disabledWithoutDocker = true)`, so on a machine or
  CI stage with no Docker it is simply **skipped**, never failed.
- **Shared connection pool.** Both approaches write through the **same HikariCP
  pool** (24 connections). This is deliberate: without pooling, the per-message
  path would be dominated by TCP connect + auth on every tiny transaction, which
  would measure connection churn instead of the thing under test — the *batching*
  difference. The pool isolates the real variable.
- **Shared workload, produced once.** `BenchmarkProducer` publishes
  **150,000 messages to one topic with 3 partitions**, spread round-robin
  across 1,500 accounts. The account number is the Kafka message key, so all
  events for an account land on the same partition and are processed in order.
  Kafka's key hashing makes the partitions uneven: 53,100 / 49,000 / 47,900
  records. Every amount is `+1`, so the expected final balance for an account is
  exactly the number of events for it — trivial to assert that nothing was lost
  or double-counted.
- **Shared store, two write paths.** `PgLedgerStore` owns the schema
  (`*_account_balance`, `*_account_change` with a `UNIQUE(topic, partition,
  offset)`, and `*_consumer_offset`). Each approach gets its own table prefix
  (`classic_*` vs `actor_*`) so their row counts and balances can be asserted
  independently against the same database.
- **What is measured.** End-to-end wall-clock (produce → all-persisted) plus
  Postgres **server-side counters** captured before and after each run:
  `pg_stat_database.xact_commit` (transactions committed) and `pg_stat_wal`
  (`wal_records`, `wal_bytes`). So the benchmark reports not just throughput but
  the **actual work each approach made the database do**.
- **Correctness gate.** Both runs must persist **every** message exactly once
  with identical, correct balances; the test asserts equal change counts, equal
  total balances (== message count), and the same set of touched accounts. Same
  result, very different DB cost.

### What the classic consumer does

`ClassicLockingConsumer` — the contention-heavy pipeline BufferFly replaces:

- One `KafkaConsumer` per partition polls records and dispatches them to a
  fixed pool of **7 worker threads per partition** (21 workers total).
- Each worker computes the account's new balance **under a per-account
  `ReentrantLock`** — the lock-based substitute for the actor's single-writer
  discipline — and persists the change via `PgLedgerStore.applyOneLocked`.
- `applyOneLocked` is **one tiny transaction per message**:
  `BEGIN; INSERT change; SELECT balance; UPSERT balance; UPDATE change;
  UPSERT offset; COMMIT` — one WAL flush and one commit per message.

This is the naive design on purpose: thousands of micro-transactions hammer
Postgres's WAL, and all workers serialize on the per-account lock for hot keys.
Correctness still holds (the lock + idempotent insert keep the sum right), which
is the whole point — *same result, far more DB work.*

### What the actor consumer does

`ActorLedgerConsumer` — the BufferFly approach:

- One `KafkaConsumer` per partition polls records and routes each to the
  `PgLedgerActor` that owns the partition via `actor.tell(...)`.
- The actor keeps the running balance **in memory in a plain `HashMap`**
  (single-writer, no locks), merges the amount, and hands a `PgLedgerChange` to
  the natural-batching persistence runner. The actor loop never touches the DB.
- The runner drains whatever has accumulated in the buffer, up to `batchSize`
  (500, the same as the consumer's `max.poll.records`) events, into **one bulk
  transaction** via
  `PgLedgerStore.applyBatch`: a batched `INSERT` of all changes and a batched
  balance upsert (one row per touched account), with the fencing offset
  advanced in the same transaction.

So N tiny commits collapse into a handful of fat ones, and the per-account
read-modify-write that the classic path pays a lock + a round-trip for is just
a `HashMap.merge` on the actor thread.

### Results

Measured on 150,000 messages over one topic with 3 partitions, Postgres 16 +
Apache Kafka in Docker, shared 24-connection HikariCP pool:

| Metric | CLASSIC (locks) | ACTOR (batching) | Advantage |
| :--- | ---: | ---: | ---: |
| consumers | 3 × 7 threads | 3 actors | — |
| **end-to-end time** | 98,050 ms | **3,453 ms** | **28.4× faster** |
| **throughput** | 1,530 msg/s | **43,440 msg/s** | **28.4×** |
| **pg `xact_commit` (Δ)** | 149,269 | **512** | **292× fewer commits** |
| pg `wal_records` (Δ) | 1,530,328 | 720,263 | 2.12× fewer |
| **pg `wal_bytes` (Δ)** | 106,938,138 | **54,968,827** | **1.95× less WAL** |

Both approaches persisted all 150,000 messages exactly once with identical,
correct balances — the benchmark fails otherwise.

> **Why these differ from earlier published numbers (10,680 ms / 1,476
> commits).** The actor used to issue one balance `UPDATE` statement per touched
> account per batch, and to detect completion by running `SELECT COUNT(*)` every
> 25 ms (each poll counted as a commit in `xact_commit`). The balance upsert is
> now a single JDBC batch and completion is counted in memory after commit.
> Re-running the old code on the same machine gave 12,508 ms and 1,434 commits;
> with only the batched upsert applied it gave 4,749 ms and 760 commits. So the
> earlier gap came from the benchmark's own write path, not from the actor.
> The figures above also include two later framework changes: natural batching
> no longer waits up to 20 ms for a batch to fill, and the container commits
> offsets with `commitAsync`.

> ⚠️ **About these numbers.** Postgres runs on the **same machine** as the test,
> so the network round-trip per transaction is near-zero. Absolute figures vary
> with hardware, JVM warmup, and Docker overhead.

### Why the actor is so much faster here

The actor does the *same work* and gets the *same result*, so the gap is
overhead the classic path pays and the actor avoids:

1. **~292× fewer commits → far less transaction overhead.** The classic path
   pays a full `BEGIN`/`COMMIT` — and the `fsync`-backed WAL flush that makes a
   commit durable — **once per message** (149,269 commits for 150k messages).
   The actor commits **once per natural batch** (512 commits). A commit is one
   of the most expensive things a relational database does; amortizing it over
   hundreds of rows instead of paying it per row is the single biggest win.

2. **Less WAL, so less I/O and less write amplification.** Every commit writes a
   commit record to the WAL. Fewer, fatter transactions mean fewer WAL records
   (2.12×) and fewer WAL bytes (1.95×) for the identical logical change.

3. **One round-trip per row disappears.** The classic path reads the current
   balance from the database inside each transaction; the actor keeps it in
   memory, so its read-modify-write is a `HashMap.merge`.

4. **The database sees one well-behaved borrower, not a thundering herd.** The
   classic path has 21 threads each holding a connection for a short, chatty
   transaction. The actor path has one borrower per partition doing a bulk write
   per batch.

This benchmark therefore mostly measures **batching vs. one transaction per
message**. The next benchmark gives the conventional design batching as well.

---

## 📊 Benchmark: actor natural-batching vs a batching locking consumer

`PostgresKafkaBatchLedgerBenchmarkTest` answers the obvious objection to the
benchmark above — that a one-transaction-per-message consumer is a straw man.
Its baseline, `LockingBatchLedgerConsumer`, is the strongest conventional design
built from standard building blocks.

### How the two are configured

| | Locking batch consumer | Actor |
| :--- | :--- | :--- |
| Kafka consumers | 1 per partition (3), `max.poll.records` 500, manual `commitSync` | 1 per partition (3), `max.poll.records` 500, auto-commit |
| Processing threads | **7 workers per partition (21)** | **1 actor per partition (3)**, plus 1 persistence thread each |
| Balance state | shared `ConcurrentHashMap`; read the balance, add, put it back under a **per-account `ReentrantLock`** | plain `HashMap` owned by the actor, **no locks** |
| DB write | **one synchronous transaction per poll**; the next poll waits for it | one transaction per natural batch (whatever accumulated, up to 500), asynchronous |
| SQL | batched change insert + batched balance upsert + offset upsert | the same batched insert + upsert, offset fenced by `FencingOffsetRepository` |
| Pipelining | fetch → process → write → commit, strictly in sequence | fetch, process and write overlap |

All three consumers share one database pool (24 connections) and the same
`PgLedgerStore`. The poll size is defined once in
`BenchmarkConsumers.MAX_POLL_RECORDS` and is also the actor's default batch
size, so a batch-consumer transaction and an actor batch cover the same number
of records.

Both keep balances in memory and start from empty tables, so neither reloads
state from the database. That is fine for a benchmark but means neither, as
written here, survives a restart mid-stream; the actor's restart path (load
state and rewind in `preStart`) is exercised by the showcase tests, not by this
benchmark.

### Key distributions

The benchmark runs three scenarios, each with its own topic of 3 partitions
and 600,000 messages keyed by account. Every account receives exactly the same
number of credits in every scenario, so final balances are asserted exactly.
Kafka hashes the keys onto partitions, so partitions are uneven:
212,400 / 196,000 / 191,600 records with 1,500 accounts, and
240,000 / 120,000 / 240,000 with 30 accounts (12 / 6 / 12 accounts).

| Scenario | Order | Accounts | Records per account per 500-record poll |
| :--- | :--- | ---: | ---: |
| round-robin | `ACC-0, ACC-1, …` repeating | 1,500 | at most 1 — workers never share an account |
| shuffled | random (fixed seed) | 1,500 | about 1, sometimes 2–3 |
| shuffled, hot keys | random (fixed seed) | 30 | about 40–80, spread over 7 workers |

Two contention measurements are reported for the batch consumer:

- **contended locks** — lock acquisitions that found the account's lock already
  held by another worker and had to wait;
- **out-of-order rows** — change rows whose `resulting_balance` does not follow
  from the previous change of the same account in Kafka offset order. Every
  amount is `+1`, so in offset order each account must read 1, 2, 3, …; the
  final balance is still correct, but the history is not in production order.

The actor must report **0 out-of-order rows** in every run (asserted): one
writer per partition applies changes in offset order.

### Results

Postgres 16 + Apache Kafka in Docker, shared 24-connection HikariCP pool. 5
rounds per scenario, alternating which approach runs first; each run uses its
own consumer group and freshly created tables, and every run must persist every
message exactly once with each account holding its exact final balance.
Medians:

| Scenario | Locking batch | Actor (batch 500) | Actor speed | Contended locks | Out-of-order rows |
| :--- | ---: | ---: | ---: | ---: | ---: |
| round-robin, 1,500 accounts | 15,870 ms | **14,790 ms** | **1.07×** | 0 | 0 |
| shuffled, 1,500 accounts | **13,078 ms** | 13,663 ms | 0.96× | 195 | 1,166 |
| shuffled, 30 accounts (hot keys) | **7,570 ms** | 9,167 ms | 0.83× | 7,982 | 53,596 |

| Median database cost | Locking batch | Actor |
| :--- | ---: | ---: |
| pg `xact_commit`, round-robin / shuffled / hot keys | 1,219 / 1,226 / 1,204 | 1,219 / 1,214 / 1,236 |
| pg `wal_bytes`, round-robin | 291,884,777 | 294,981,253 |

**Where the time goes** — per thread (sum over the 3 partitions ÷ 3), as a
share of the median run:

| Phase | Round-robin | Shuffled | Hot keys |
| :--- | ---: | ---: | ---: |
| Lock consumer: in-memory processing (parse + locked update) | 1.9% | 2.1% | 2.5% |
| Lock consumer: database transaction (blocks the next poll) | 89.1% | 87.2% | 75.1% |
| Lock consumer: `commitSync` (blocks the next poll) | 2.0% | 1.9% | 2.6% |
| Actor poll thread: JSON parse | 1.8% | 1.5% | 2.0% |
| Actor poll thread: hand-off to the actor (`tell`) | 1.0% | 1.4% | 1.9% |
| Actor persistence thread: database transaction | 91.5% | 92.8% | 83.0% |

With hot keys the partitions are uneven (240,000 / 120,000 / 240,000), so the
small partition's threads sit idle for part of the run and the per-thread
shares are lower.

**Smaller actor batches** (`-Dbufferfly.bench.actorBatchSize=250`, polls stay
at 500): the actor commits twice as often (~2,370 vs ~1,245). Separate run,
medians:

| Scenario | Locking batch | Actor (batch 250) | Actor speed |
| :--- | ---: | ---: | ---: |
| round-robin, 1,500 accounts | **9,733 ms** | 10,675 ms | 0.91× |
| shuffled, 1,500 accounts | **8,480 ms** | 9,927 ms | 0.85× |
| shuffled, 30 accounts (hot keys) | **8,211 ms** | 10,274 ms | 0.80× |

The locking consumer runs with identical settings in both runs, yet its
round-robin median was 15,870 ms in the first and 9,733 ms in the second: on
this machine the run-to-run noise is larger than the difference between the
two designs.

### What this shows

- **Database cost is identical.** With the same batch size and the same SQL,
  both commit about once per 500 messages and write the same WAL. Once the
  conventional design batches, the database does the same work.
- **The database is the bottleneck for both.** In-memory work is 2–5% of the
  run on either side, so removing locks or overlapping fetch with writing can
  only save a few percent. With one consumer per partition, both designs have
  exactly 3 writers with one transaction in flight each.
- **Lock contention is real but cheap.** With 30 hot accounts about 8,000 lock
  acquisitions per run had to wait, but a lock is held only for a map read and
  write, so waiting costs almost nothing.
- **Ordering is where the designs differ.** The lock consumer's workers apply
  one account's updates in a different order than Kafka produced them (53,596
  out-of-order change rows with hot keys); the actor never does.
- **Results are noisy.** Single rounds of the same configuration vary by up to
  ~35%, and the same approach's median moved by ~60% between two runs. Treat
  differences under ~20% as noise. In these runs the locking consumer was
  ahead with hot keys (0.83× and 0.80×); why is not measured.
- **What the actor buys.** Single-writer, offset-ordered state per partition
  with no locks, per-partition backpressure that pauses only the slow
  partition, and a supervised restart that rewinds the offset from the
  database. The batch consumer has none of these: a slow write blocks every
  partition on that consumer, and a failure needs hand-written recovery.

### Not measured yet

- **A remote database.** Every synchronous write and `commitSync` blocks the
  batch consumer's next poll, so round-trip latency costs it more than it costs
  the actor, whose fetching overlaps with writing.
- **Many partitions per consumer.** The actor writes each partition on its own
  persistence thread; the batch consumer writes all of a poll's partitions in
  one transaction on one thread.
- **A consumer that keeps no state in memory.** The common conventional
  design reads the touched balances from the database on every poll (select,
  calculate, batch update/insert) because it cannot trust a cache across
  rebalances. The actor can load its partition's state once on assignment and
  keep it, which this benchmark does not exercise.

---

## 📊 Benchmark: sharded actors vs Apache Flink (keyed state + JDBC sink)

`FlinkVsShardedActorLedgerBenchmarkTest` runs the same ledger workload through
**Apache Flink 2.2.1** and through BufferFly's `ShardedActorContainer`: same
Kafka topics, same Postgres tables, same batch size (500), balances kept in
memory on both sides.

### How the two are built

| | Apache Flink | BufferFly |
| :--- | :--- | :--- |
| Source | bounded `KafkaSource` (earliest → offsets at start), `max.poll.records` 500 | one `ShardedActorContainer` on the topic, `concurrency` 3 (one consumer per partition), `max.poll.records` 500 |
| Routing | `keyBy(account)` — records are hashed by account across subtasks | one actor per partition |
| State | balance in keyed `ValueState` (default heap state backend) | balance in the actor's `HashMap` |
| DB write | `JdbcSink`, at-least-once, JDBC batches of 500; one statement per change that inserts the change and upserts the balance | natural batching, up to 500 changes per transaction; batched change insert + one balance upsert per touched account + fenced offset |
| Writers | one sink connection per subtask | one persistence thread per partition (3) |
| Settings | Flink defaults: local environment, checkpointing off; parallelism = default (22, one per core here) or 3 | defaults |

Flink is run twice: at its **default parallelism** (22 subtasks, 22 database
writers on this machine) and at **parallelism 3**, which matches the actor
side's 3 writers. Each scenario's rounds rotate the order of the three
contenders; medians of 3 rounds are reported. "Steady" is from the first
record processed to the last change committed; "wall" also includes Flink's
local cluster start-up and job submission, or the container's consumer-group
join.

Every run must persist each change exactly once with every account's exact
final balance and **no change row out of Kafka offset order** — all three pass
in every round (Flink's `keyBy` keeps each account's events in order because
each account comes from a single partition).

### Results

600,000 messages per scenario on one topic with 3 partitions, keyed by
account (the same layout and uneven partition sizes as the batch benchmark),
Postgres 16 + Apache Kafka in Docker. Medians, steady-state time:

| Scenario | BufferFly (3 writers) | Flink, parallelism 3 | Flink, default parallelism (22) |
| :--- | ---: | ---: | ---: |
| round-robin, 1,500 accounts | 17,063 ms | 13,934 ms | **6,736 ms** |
| shuffled, 1,500 accounts | 12,574 ms | 13,875 ms | **6,107 ms** |
| shuffled, 30 accounts (hot keys) | 9,092 ms | 12,481 ms | **6,617 ms** |

| Median database cost (round-robin / shuffled / hot keys) | BufferFly | Flink p3 | Flink p22 |
| :--- | ---: | ---: | ---: |
| pg `xact_commit` | 1,199 / 1,195 / 1,144 | 2,410 / 2,410 / 2,435 | 2,502 / 2,477 / 2,452 |
| pg `wal_bytes` (MB) | 286 / 256 / 200 | 291 / 291 / 288 | 307 / 298 / 292 |

This run was noisy: the machine was busier than during an earlier run of the
same code, in which every contender was 30–50% faster (steady medians:
BufferFly 9,796 / 10,779 / 7,118 ms, Flink p3 9,151 / 10,366 / 10,913 ms, Flink
default 3,624 / 3,386 / 5,144 ms). Single BufferFly rounds in the latest run
ranged 12.8–17.6 s on round-robin.

### What this shows

- **With the same number of writers, BufferFly and Flink are close on 1,500
  accounts, and BufferFly is faster with hot keys** (~1.4× in the latest run,
  ~1.5× in the earlier one). With 30 accounts Flink at parallelism 3 did not
  get faster, while BufferFly did. The cause is not measured. It is not key
  skew: Flink's key-group assignment puts the 30 accounts on its 3 subtasks as
  12 / 11 / 7. A likely candidate is the sink's per-change balance upsert — a
  500-statement batch updating the same few rows — but that is not verified.
- **Flink at default parallelism is fastest**, because it writes to Postgres
  over 22 connections at once, against BufferFly's 3 (one per partition). That
  is parallelism, not per-writer speed; BufferFly's parallelism comes from
  partitions, so more partitions give it more writers.
- **BufferFly makes Postgres do less work.** It commits once per batch
  (~1,150–1,200 commits) and upserts each touched account once per batch.
  Flink's JDBC sink commits about twice as often (~2,400–2,500) — likely
  because the Postgres JDBC driver runs a 500-statement batch in auto-commit as
  more than one implicit transaction; not verified — and it upserts the balance
  once per change. With hot keys that is ~30% more WAL for the same result.
- **Rebalance at start-up is lossless.** The 3 consumers join the group at
  slightly different times, so partitions move once while it forms. Each
  BufferFly actor loads its partition's balances and stored offset in
  `preStart` and seeks there; because starting an actor blocks until
  `preStart` has finished, no record reaches it before that rewind. (Before
  this was fixed, records polled from the old position could be applied on top
  of the reloaded state, which inflated balances; the benchmark's exact-balance
  check caught it.)
- **Start-up cost.** Flink's local cluster start and job submission add about
  0.5–1.4 s to each run; BufferFly's consumer-group join adds about 20–30 ms.
  This matters for short runs, not for long-running jobs.
- **Not compared here:** Flink's checkpointing and exactly-once sink (both off
  by default; BufferFly gets exactly-once from its DB-stored offset fence), a
  RocksDB state backend, a real Flink cluster, and a remote database.

---

## 📊 Benchmark: `ShardedActorContainer` throughput (Kafka → actors, no database)

The other Kafka benchmarks drive actors from a plain `KafkaConsumer`, so they do
not exercise the Spring container. `ShardedActorContainerThroughputBenchmarkTest`
does: a single consumer (`concurrency = 1`) owns all 3 partitions of one topic,
so every poll is split per partition, routed through each partition's rewind
gate to its actor, and committed with `commitAsync`. The actors only count
records, so the measured time is the container's poll → route → commit path.

300,000 messages, 3 rounds, median:

| Version | Steady-state throughput |
| :--- | ---: |
| per-record listener, `MANUAL_IMMEDIATE` ack per record (previous release) | ~8,800 msg/s |
| batch listener, `commitSync` per poll | ~377,000 msg/s |
| **batch listener, `commitAsync` per poll (current)** | **~400,000 msg/s** |

Steady state is measured from the first record to the last; including the
consumer-group join, the current version takes ~780 ms for the 300,000 records.
The first two rows come from earlier runs of the older code and the current row
from the latest run (rounds ranged 272,000–426,000 msg/s; an earlier run of the
same version measured ~468,000), so compare the orders of magnitude, not the
last two rows exactly.

### Run them yourself

Requires Docker to be running (the tests skip cleanly without it):

```bash
./gradlew :bufferfly-benchmarks:benchmarks --tests '*PostgresKafkaLedgerBenchmarkTest*'
./gradlew :bufferfly-benchmarks:benchmarks --tests '*PostgresKafkaBatchLedgerBenchmarkTest*'
./gradlew :bufferfly-benchmarks:benchmarks --tests '*PostgresKafkaBatchLedgerBenchmarkTest*' -Dbufferfly.bench.actorBatchSize=250
./gradlew :bufferfly-benchmarks:benchmarks --tests '*ShardedActorContainerThroughputBenchmarkTest*'
./gradlew :bufferfly-benchmarks:benchmarks --tests '*FlinkVsShardedActorLedgerBenchmarkTest*'
```

The workload size is tunable at the top of each test class (`PARTITIONS`,
`MESSAGES`, and `ACCOUNTS` or the scenario list, plus `ROUNDS` for the batch
and Flink benchmarks). The poll size is `BenchmarkConsumers.MAX_POLL_RECORDS`; the actor's
batch size defaults to it and can be overridden with
`-Dbufferfly.bench.actorBatchSize`.

---

## License

BufferFly is open-source software licensed under the
[Apache License, Version 2.0](../LICENSE).
