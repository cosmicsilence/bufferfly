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
- [📊 Benchmarks](#-benchmarks)

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

## 📊 Benchmarks

The benchmarks for this module — actor vs a classic and a batching locking
consumer, sharded actors vs Apache Flink, and the container's own throughput —
run against a real Postgres and Kafka in Docker. Their setup, results and
caveats are in the [benchmarks README](../bufferfly-benchmarks/README.md); the
headline numbers are in the [main README](../README.md#performance).

---

## License

BufferFly is open-source software licensed under the
[Apache License, Version 2.0](../LICENSE).
