# 🦋 BufferFly

![Build Status](https://github.com/cosmicsilence/bufferfly/actions/workflows/build.yml/badge.svg)
[![Latest Version](https://img.shields.io/maven-central/v/io.github.cosmicsilence/bufferfly-core)](https://central.sonatype.com/artifact/io.github.cosmicsilence/bufferfly-core)
[![Java](https://img.shields.io/badge/Java-25+-orange.svg)](https://adoptium.net/temurin/releases/?version=25)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

BufferFly is a lightweight, virtual-thread-native **actor framework for Java 25+** that acts as an
**in-memory shock absorber and natural batching engine** in front of high-volume databases — most
commonly between a Kafka consumer and a relational store.

---

## Modules

| Module | What it is | Use it when |
| :--- | :--- | :--- |
| 📦 **bufferfly-core** | The virtual-thread actor engine: actors, mailboxes, the `VTDispatcher`, the persistence/batching runner, and the backpressure primitives. | Almost always. General-purpose, massively scalable, near-zero idle cost. |
| 🏎️ **bufferfly-cpu-bound** | A hardware-pinned, busy-spinning dispatcher (`SpinningActorDispatcher`) built on JCTools MPSC queues and thread affinity. | Only for a handful of ultra-high-frequency, sub-microsecond-latency actors. |
| ⚡ **bufferfly-spring-kafka** | Spring Boot auto-configuration that wires BufferFly actors to Kafka partitions via the `@ShardedActor` annotation: one actor per partition, DB-stored offsets, backpressure that pauses partitions, and actor restart with offset rewind. | You run on Spring + Kafka and want the shock absorber wired for you. |

---

## The problem: Kafka + mini-transactions + connection pools under pressure

A very common ingestion pipeline looks like this:

```
Kafka partition ──► consumer ──► for each record: BEGIN; INSERT; COMMIT ──► Postgres
```

Under a burst this design falls apart, and it fails in a way that is painful to debug:

- **One tiny transaction per message.** Each record pays the full cost of a round-trip, a
  transaction begin/commit, WAL flush, and index maintenance. At 50k msg/s that is 50k
  transactions/s — the database spends most of its time on transaction overhead, not on your data.
- **Connection-pool exhaustion.** To go faster you add consumer threads; each needs a pooled
  connection. The burst drains the pool, callers block waiting for a connection, latency spikes,
  health checks start failing, and the pool's queue backs up into the JVM heap.
- **No real backpressure.** Kafka keeps handing you records as fast as you can `poll()`. If the DB
  slows down, the work piles up *in your application* (unbounded queues, growing heap) until an
  `OutOfMemoryError` or a cascading timeout storm takes the service down.
- **All-or-nothing offset commits.** If you commit offsets before the data is durable you lose
  messages on a crash; if you commit after, a crash mid-batch replays and double-writes. Getting
  exactly-once against a database is subtle and easy to get wrong.

The database would happily absorb the same load as **a few large bulk inserts** instead of millions
of tiny transactions — it just needs something in front of it to **coalesce** the writes and to
**push back** when it is behind.

## The solution: an actor as a shock absorber

BufferFly puts a thin in-memory layer between the stream and the database:

```
Kafka ──► Actor (in-memory, single-threaded, CPU-bound) ──► PersistenceRunner ──► DB
              │  no I/O, no locks, no blocking                 │  natural batching
              │  one actor per partition → ordered             │  one bulk write per batch
```

- **The actor loop is pure and non-blocking.** It never touches the database. It transforms each
  message and hands the result to the persistence layer. One actor owns a partition, so per-key
  ordering is preserved without locks.
- **The persistence runner does natural batching.** It drains the buffer and coalesces up to
  `batchSize` events — *or* whatever has accumulated within `timeoutMs` — into a **single bulk
  transaction**. Under load the batch is always full (maximum DB efficiency); under light load the
  timeout keeps latency bounded. Millions of tiny transactions become thousands of fat ones.
- **The buffer is the shock absorber.** Bursts are absorbed in memory and smoothed into a steady
  write rate the database can sustain. The connection pool sees one borrower doing bulk writes, not
  a thundering herd.
- **Backpressure is first-class.** When the buffer fills past a high watermark, BufferFly pushes
  back — on Spring + Kafka it *pauses the partition* so the broker stops delivering until the buffer
  drains back below a low watermark.

### Persistence model

`AbstractPersistentActor<T, E>` splits the work cleanly:

```java
public final class LedgerActor extends AbstractPersistentActor<ConsumedRecord<AddToAccount>, LedgerChange> {

    @Override
    public PersistenceRunner.PersistenceEvent<LedgerChange> apply(ConsumedRecord<AddToAccount> record) {
        // pure, in-memory transform — runs on the single actor thread, no I/O
        long newBalance = balances.merge(record.value().accountNumber(), record.value().amount(), Long::sum);
        return PersistenceRunner.newEvent(new LedgerChange(...), new BalanceApplied(...)); // + optional side effect
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        // runs AFTER the batch is durably committed — the place for post-commit projections/notifications
        return sideEffect -> { /* update a read model, emit an event, ... */ };
    }
}
```

`TransactionalOperations<E>` is where you implement the **bulk write** and classify failures:

- throw `TransientException` → the whole batch is retried with exponential back-off;
- throw `SingleItemException` → the runner isolates the batch item-by-item, persists the good ones,
  and calls `onPoisonPill` for the offender (dead-letter it);
- throw `UnrecoverableException` → the runner halts and invokes the `onUnrecoverable` hook (in the
  Spring module this restarts the actor and rewinds the offset).

---

## Performance

BufferFly is engineered with strict mechanical sympathy. There are two independent performance
stories: **batching throughput to the database** (core) and **raw actor dispatch throughput**
(core vs. cpu-bound).

### 📊 Batching vs. one-transaction-per-message

`PersistenceActorBenchmarkTest` compares a BufferFly persistent actor (natural batching) against the
naive approach of N virtual threads each doing one `INSERT` per message, over 100,000 rows into
HSQLDB. The actor coalesces those writes into a handful of bulk transactions, turning per-message
transaction overhead into a near-constant cost — exactly the pain described above, measured.

| Approach | Time for 100,000 rows | Throughput | Transactions |
| :--- | :---: | :---: | :--- |
| **Actor + natural batching** | **~240 ms** | **~400,000 rows/s** | ~101 bulk transactions (≈990 rows each) |
| 15 virtual threads, one INSERT per row | ~820 ms | ~120,000 rows/s | 100,000 tiny transactions |
| | | | **≈ 3.2–3.5× faster** |

> ⚠️ **About these numbers.** The benchmark runs against an **in-memory HSQLDB in the same JVM** — so
> there is **no network round-trip and no connection-pool contention**. That deliberately *understates*
> BufferFly's real-world advantage: the pain it removes (per-message round-trips, transaction
> begin/commit, WAL flush, connection-pool exhaustion) is largely absent in a local in-memory DB. Over
> a network to a real database (Postgres, etc.), where each transaction costs a round-trip and holds a
> pooled connection, collapsing 100,000 transactions into ~100 bulk writes is far more dramatic than
> the ~3.3× seen here. Treat this as a conservative lower bound that isolates the batching effect
> itself. Absolute figures vary with hardware and JVM warmup.

> Run it yourself: `./gradlew :bufferfly-core:test --tests '*PersistenceActorBenchmarkTest*'`

### 📊 Firehose: actor dispatch throughput

End-to-end processing of a burst of 5,000,000 messages to a single target actor path
(`DispatcherComparisonBenchmarkTest`):

| Upstream Producers | bufferfly-core<br>*(Virtual Threads)* | bufferfly-cpu-bound<br>*(Hardware-Pinned Core)* | Speedup |
| :--- | :---: | :---: | :---: |
| **1 producer**   | ~5.5M msg/s | **⚡ 14.25M msg/s** | **2.59×** |
| **10 producers** | ~4.8M msg/s | **⚡ 12.68M msg/s** | **2.64×** |
| **50 producers** | ~4.4M msg/s | **⚡ 13.82M msg/s** | **3.14×** |

> Single-producer pipelines frequently cross **15M msg/s** on the spinning engine because
> hardware cache-line invalidation is eliminated.

### 📦 bufferfly-core — the virtual thread engine

- **Blueprint:** a `VTDispatcher` with a bounded `LinkedBlockingQueue` mailbox per actor, handing
  execution to the JVM's virtual-thread carrier pool. One single-consumer slot per actor guarantees
  the single-threaded-actor invariant.
- **Ceiling:** ~4.5M–5.5M msg/s, bounded by virtual-thread continuation and stack-frame overhead.
- **Idle cost:** ~zero. Thousands of actors co-exist with no dedicated threads and no busy-spin.
- **Best for:** business state machines, webhook ingestion, and standard Kafka partition processing.

### 🏎️ bufferfly-cpu-bound — the hardware-pinned spinning engine

- **The 15M mark:** a core-pinned infinite busy-spin thread (`SpinningActorDispatcher`) over a
  JCTools MPSC `LOCK XADD` mailbox eliminates the CAS-retry cliff, reaching 14.25M+ msg/s.
- **Self-healing affinity:** an amortized bitwise-mask counter (`& 0xFFFF`) re-checks core placement
  once every 65,536 cycles; if the OS migrates the thread it snaps back to its original core with no
  heavy JNI overhead.
- **Best for:** financial order books, market tickers, sub-microsecond tail-latency SLAs.

### ⚠️ When NOT to use BufferFly (and when not to use the spinning engine)

**Do not reach for BufferFly when:**

- **You need per-message, synchronous, read-your-write durability** (e.g. "the HTTP response must
  return the committed row id"). Batching is asynchronous by nature; a request/response store is a
  better fit.
- **Your volume is low.** If the database comfortably handles one transaction per message, the
  batching layer is complexity you don't need.
- **You require cross-key/global transactional guarantees** spanning many partitions. BufferFly's
  ordering and offset guarantees are **per partition**.
- **Your workload is dominated by blocking I/O inside the actor.** Actors must stay CPU-bound and
  non-blocking; blocking in `receive`/`apply` defeats the model.

**Do not use `bufferfly-cpu-bound` (spinning dispatcher) unless:**

- you have a **small, fixed** number of ultra-hot actors and **isolated physical cores** to spare —
  each spinning actor pins one core at **100% CPU forever**, even while idle;
- you genuinely need sub-microsecond dispatch latency. For everything else the virtual-thread engine
  is cheaper and simpler. The spinning engine deliberately supports **only** the `THROW` backpressure
  strategy (never `CALLER_RUNS`), because running actor work on a producer thread would destroy its
  cache-resident latency profile.

---

## ⚡ bufferfly-spring-kafka

Spring Boot auto-configuration that turns a plain class into a sharded, persistent, backpressure-aware
Kafka consumer. You write the actor; BufferFly wires the consumer, the lifecycle, and the recovery.

### Setup

Add the dependency (Spring Boot's Kafka auto-configuration provides the `ConsumerFactory` from your
`spring.kafka.*` properties):

```groovy
implementation 'io.github.cosmicsilence:bufferfly-spring-kafka'
```

Annotate a factory bean with `@ShardedActor` and implement `ShardedActorFactory`:

```java
@Component
@ShardedActor(
        name          = "ledger",
        topic         = "ledger-events",
        concurrency   = 3,        // Kafka listener concurrency (consumers in the group)
        highWatermark = 50_000,   // engage backpressure at this buffer depth
        lowWatermark  = 20_000)   // release backpressure here (hysteresis)
public class LedgerShardFactory implements ShardedActorFactory<AddToAccount> {

    private final LedgerOperations operations;          // your TransactionalOperations
    private final BalanceConfirmationService read Model; // any collaborator

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

That is all the wiring you write. BufferFly's `ShardedActorBeanPostProcessor` discovers the annotated
bean and stands up a `ConcurrentMessageListenerContainer` for you.

### How it works & the functionalities you get

**One actor per partition (rebalance-driven lifecycle).** On `onPartitionsAssigned` the framework
builds a `KafkaContext`, asks your factory for an actor, and starts it. On
`onPartitionsRevoked`/`onPartitionsLost` it stops that actor. Per-partition ordering is preserved and
a reassigned partition cleanly gets a fresh actor.

**Records are delivered as `ConsumedRecord<V>`** — the deserialized value plus its `topic`,
`partition`, and `offset` — so a persistent actor always has the offset available for the
fencing-token pattern below.

**No message loss (manual ack).** The container uses `MANUAL_IMMEDIATE` acks via a dedicated
`ShardedActorConsumer`. Durability does **not** rely on the Kafka ack; it relies on the actor storing
the consumed **offset in the same database transaction as the data**. On restart the actor seeks back
to its DB-stored offset, so anything acked to Kafka but not committed to the DB is simply re-consumed.

**Offset as an optimistic-locking fencing token.** The persistence layer stores the latest processed
offset (per topic/partition) and advances it with a compare-and-set (`WHERE latest_offset = :expected`)
in the same transaction as the data. If the CAS fails, a **zombie actor** (an old instance still
running after a rebalance) is racing the new owner → the batch is rejected as `UnrecoverableException`.

**Exactly-once sink.** Writes are idempotent by `(topic, partition, offset)` — a record that is
re-delivered after a restart/rewind is skipped rather than double-inserted, and the running balance is
only adjusted for newly-inserted rows. No loss, no duplicates.

**Backpressure that pauses the partition.** Through `KafkaContext` an actor's persistence layer can
`pausePartition()` when its buffer crosses the high watermark and `resumePartition()` once it recedes
to the low watermark — fetching stops for that partition only, not the whole container. As a reactive
safety net, if `tell` throws `UnderPressureException` or `MailBoxRefusedException` the consumer pauses
the partition and leaves the record un-acked for redelivery. **No message is ever dropped under
pressure — the broker is simply told to wait.**

**Signal-based, thread-safe seeks.** Kafka's consumer is single-threaded, so an actor never touches it
directly. `KafkaContext.seek(...)`/`seekToBeginning()` record a *signal* that the framework applies on
the consumer thread (via Spring's `ConsumerSeekAware` callback) before the next poll; records left over
from a poll taken before a rewind are discarded so a stale offset can never slip through.

**Persistence failure → actor restart → offset rewind.** When the runner hits an `UnrecoverableException`
(e.g. a failed zombie fence) it halts and calls your `onUnrecoverable` hook. Wiring it to
`ctx.restartActor()` stops the current actor and starts a fresh one, whose `preStart` rehydrates its
in-memory state and **seeks back to the last committed offset** — a clean, from-known-good restart with
no double counting and no loss.

**Poison pills → dead-letter + continue.** A record the database can never accept (e.g. a field too
long) surfaces as a real DB data-violation, which the operations layer classifies as
`SingleItemException`. The runner isolates the batch item-by-item: the good records before *and* after
the poison pill persist, and the offender is written to a dead-letter table. Nothing around it is lost.

**Post-commit side effects.** `PersistenceEvent`s can carry a `SideEffect` that the runner runs only
**after** the batch is durably committed — the right place to update a read model, warm a cache, or
emit a downstream event, with the guarantee it never runs ahead of the database.

See `bufferfly-spring-kafka/src/test/java/io/bufferfly/spring/ledger/` for a full, runnable showcase:
a sharded account **ledger** on embedded Kafka that keeps balances in memory, batches changes to
HSQLDB, stores the offset as a fencing token, dead-letters an oversized message, and restarts +
rewinds — all wired entirely by Spring through `@ShardedActor`.

---

## Building

```bash
./gradlew build          # compile + test all modules
./gradlew :bufferfly-core:test
./gradlew :bufferfly-spring-kafka:test
```

Requires a Java 25 toolchain.

## 📄 License

BufferFly is open-source software licensed under the [Apache License, Version 2.0](LICENSE).
