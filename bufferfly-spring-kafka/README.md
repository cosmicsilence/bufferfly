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
- [What you get, wired for free](#what-you-get-wired-for-free)
- [📊 Benchmark: actor natural-batching vs classic locking consumer](#-benchmark-actor-natural-batching-vs-classic-locking-consumer)
  - [How the benchmark is built](#how-the-benchmark-is-built)
  - [What the classic consumer does](#what-the-classic-consumer-does)
  - [What the actor consumer does](#what-the-actor-consumer-does)
  - [Results](#results)
  - [Why the actor is dramatically faster](#why-the-actor-is-dramatically-faster)
  - [Run it yourself](#run-it-yourself)

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
                .timeoutMs(20L)
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

## What you get, wired for free

| Capability | What BufferFly does |
| :--- | :--- |
| **One actor per partition** | On `onPartitionsAssigned` it builds a `KafkaContext`, asks your factory for an actor, and starts it; on revoke/loss it stops that actor. Per-partition ordering is preserved — one single-writer actor owns a partition. |
| **Records as `ConsumedRecord<V>`** | The deserialized value plus `topic`, `partition`, `offset`, so a persistent actor always has the offset for the fencing-token pattern. |
| **No message loss (manual ack)** | `MANUAL_IMMEDIATE` acks; durability relies not on the Kafka ack but on storing the consumed **offset in the same DB transaction as the data**. On restart the actor seeks back to its DB-stored offset. |
| **Offset as a fencing token** | The stored offset is advanced with a compare-and-set (`WHERE latest_offset = :expected`) in the same transaction. A failed CAS means a zombie actor is racing the new owner → the batch is rejected as `UnrecoverableException`. |
| **Exactly-once sink** | Writes are idempotent by `(topic, partition, offset)` — a re-delivered record is skipped, never double-inserted. |
| **Backpressure that pauses the partition** | When the buffer crosses the high watermark, `KafkaContext.pausePartition()` stops fetching for that partition only; `resumePartition()` fires once it recedes to the low watermark. |
| **Restart + rewind on fatal failure** | `onUnrecoverable` → `ctx.restartActor()` stops the current actor and starts a fresh one that rehydrates in-memory state and seeks back to the last committed offset. |
| **Poison-pill dead-lettering** | A record the DB can never accept is classified as `SingleItemException`; the runner isolates the batch item-by-item, persists the good records, and dead-letters the offender. |
| **Post-commit side effects** | `PersistenceEvent`s can carry a `SideEffect` that runs only **after** the batch is durably committed — the right place to update a read model or emit a downstream event. |

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
  **50,000 messages to each of 3 topics (150,000 total)**, spread round-robin
  across 500 accounts per topic. The account number is the Kafka message key, so
  all events for an account land on the same partition and are processed in
  order. Every amount is `+1`, so the expected final balance for an account is
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

- One `KafkaConsumer` per topic polls records and dispatches them to a fixed
  pool of **7 worker threads per topic** (21 workers total).
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

- One `KafkaConsumer` per topic polls records and routes each to the single
  `PgLedgerActor` that owns its `(topic, partition)` via `actor.tell(...)`.
- The actor keeps the running balance **in memory in a plain `HashMap`**
  (single-writer, no locks), merges the amount, and hands a `PgLedgerChange` to
  the natural-batching persistence runner. The actor loop never touches the DB.
- The runner drains the buffer and coalesces up to `batchSize` (500) events —
  or whatever accumulated within `timeoutMs` (20 ms) — into **one bulk
  transaction** via `PgLedgerStore.applyBatch`: a batched `INSERT` of all
  changes, one balance upsert per account, and a single offset advance, all in
  the same transaction.

So N tiny commits collapse into a handful of fat ones, and the per-account
read-modify-write that the classic path pays a lock + a round-trip for is just
a `HashMap.merge` on the actor thread.

### Results

Measured on 150,000 messages over 3 topics (1 partition each), Postgres 16 +
Apache Kafka in Docker, shared 24-connection HikariCP pool:

| Metric | CLASSIC (locks) | ACTOR (batching) | Advantage |
| :--- | ---: | ---: | ---: |
| consumers | 3 × 7 threads | 3 actors | — |
| **end-to-end time** | 92,630 ms | **10,680 ms** | **8.67× faster** |
| **throughput** | 1,619 msg/s | **14,045 msg/s** | **8.67×** |
| **pg `xact_commit` (Δ)** | 149,254 | **1,476** | **101× fewer commits** |
| pg `wal_records` (Δ) | 1,533,170 | 867,652 | 1.77× fewer |
| **pg `wal_bytes` (Δ)** | 108,930,618 | **67,772,333** | **1.61× less WAL** |

Both approaches persisted all 150,000 messages exactly once with identical,
correct balances — the benchmark fails otherwise.

> ⚠️ **About these numbers.** Postgres runs on the **same machine** as the test,
> so the network round-trip per transaction is near-zero. That *understates* the
> real-world gap: over a network to a managed database, where each transaction
> costs a real round-trip and holds a pooled connection for its duration,
> collapsing ~149k commits into ~1.5k is far more dramatic than the ~8.7× seen
> here. Treat this as a conservative lower bound. Absolute figures vary with
> hardware, JVM warmup, and Docker overhead.

### Why the actor is dramatically faster

The actor does the *same work* and gets the *same result*, so the gap is pure
overhead the classic path pays and the actor avoids:

1. **~101× fewer commits → far less transaction overhead.** The classic path
   pays a full `BEGIN`/`COMMIT` — and the `fsync`-backed WAL flush that makes a
   commit durable — **once per message** (149,254 commits for 150k messages).
   The actor commits **once per natural batch** (1,476 commits). A commit is one
   of the most expensive things a relational database does; amortizing it over
   ~100 rows instead of paying it per row is the single biggest win.

2. **Less WAL, so less I/O and less write amplification.** Every commit writes a
   commit record to the WAL, and small transactions amplify WAL via per-commit
   records and full-page writes. Fewer, fatter transactions mean fewer WAL
   records (1.77×) and fewer WAL bytes (1.61×) for the identical logical change —
   less disk I/O and less background churn on the database.

3. **No lock contention on hot accounts.** With 500 accounts and 21 worker
   threads, the classic path constantly has multiple workers trying to update the
   same account; they **serialize on the per-account `ReentrantLock`**, so extra
   threads do not buy extra throughput on skewed keys — they just wait. The actor
   has **no locks at all**: one single-writer actor owns the partition's state and
   mutates a plain `HashMap`. The balance read-modify-write is an in-memory map
   operation, not a locked DB round-trip.

4. **The database sees one well-behaved borrower, not a thundering herd.** The
   classic path has 21 threads each holding a connection for a short, chatty
   transaction, maximizing connection churn and WAL contention. The actor path
   has one borrower doing a big bulk write per batch — exactly the access pattern
   a relational store is happiest with.

In short: the classic path makes Postgres do ~150k transactions' worth of
commit/WAL/lock work; the actor makes it do ~1.5k. **Postgres suffers from the
commits and the WAL, not from the data** — and natural batching is what removes
that suffering while preserving ordering and exactly-once semantics.

### Run it yourself

Requires Docker to be running (the test skips cleanly without it):

```bash
./gradlew :bufferfly-spring-kafka:test --tests '*PostgresKafkaLedgerBenchmarkTest*'
```

The workload size is tunable at the top of `PostgresKafkaLedgerBenchmarkTest`
(`MESSAGES_PER_TOPIC`, `ACCOUNTS_PER_TOPIC`, `ACTOR_BATCH_SIZE`,
`ACTOR_TIMEOUT_MS`).

---

## License

BufferFly is open-source software licensed under the
[Apache License, Version 2.0](../LICENSE).
