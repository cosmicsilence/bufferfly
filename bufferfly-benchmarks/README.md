# 🦋 BufferFly benchmarks

Test-only module (never published) with the benchmarks behind the numbers in the
[main README](../README.md#performance). Each benchmark is a JUnit test run by
the `benchmarks` Gradle task, in its own JVM. The database and Kafka benchmarks
start a real **Postgres 16** and a real **Apache Kafka** broker in Docker via
Testcontainers, and are skipped, never failed, when Docker is not available.

> ⚠️ **About all numbers here.** Postgres and Kafka run in Docker on the **same
> machine** as the test, so database round-trips are near zero. Absolute
> figures vary with hardware, JVM warmup, Docker overhead and whatever else the
> machine is doing; single rounds of the same configuration vary by up to
> ~35%. Treat differences under ~20% as noise.

## Contents

- [📊 Batching vs one transaction per row](#-benchmark-batching-vs-one-transaction-per-row)
- [📊 Actor dispatch throughput (firehose)](#-benchmark-actor-dispatch-throughput-firehose)
- [📊 Actor natural-batching vs classic locking consumer](#-benchmark-actor-natural-batching-vs-classic-locking-consumer)
- [📊 Actor natural-batching vs a batching locking consumer](#-benchmark-actor-natural-batching-vs-a-batching-locking-consumer)
- [📊 Sharded actors vs Apache Flink](#-benchmark-sharded-actors-vs-apache-flink-keyed-state--jdbc-sink)
- [📊 `ShardedActorContainer` throughput](#-benchmark-shardedactorcontainer-throughput-kafka--actors-no-database)
- [Run them yourself](#run-them-yourself)

---

## 📊 Benchmark: batching vs one transaction per row

`PersistenceActorBenchmarkTest` compares a BufferFly persistent actor (natural batching) against the
naive approach of 15 virtual threads each doing one `INSERT` per row, over 100,000 rows into a
**real Postgres** (a throwaway Docker container via Testcontainers). Both paths write through the
**same HikariCP pool** and the same table/SQL, so the only variable is *how* each drives the database;
beyond wall-clock it captures Postgres's own `pg_stat_database.xact_commit` and `pg_stat_wal` counters,
so the commit/WAL cost of the per-row path is visible next to the actor's bulk transactions.

| Metric | Actor + natural batching | 15 virtual threads, one INSERT per row | Advantage |
| :--- | :---: | :---: | :---: |
| **time for 100,000 rows** | **~1,155 ms** | ~12,219 ms | **~10.6× faster** |
| **throughput** | **~86,500 rows/s** | ~8,180 rows/s | **~10.6×** |
| **pg `xact_commit`** | **476** (103 bulk tx) | 98,505 | **~207× fewer commits** |
| pg `wal_records` | 87,457 | 412,117 | ~4.7× fewer |
| **pg `wal_bytes`** | **8,484,851** | 33,862,017 | **~4.0× less WAL** |

The actor coalesces the 100,000 writes into ~103 bulk transactions, turning per-row transaction
overhead (begin/commit + WAL flush) into a near-constant cost, measured against a real database.

> ⚠️ **About these numbers.** Postgres runs on the **same machine** as the test, so the network
> round-trip per transaction is near-zero — which *understates* the advantage. Over a network to a
> managed database, where each commit costs a real round-trip and holds a pooled connection,
> collapsing ~98k commits into ~500 is far more dramatic than the ~10.6× seen here. Treat this as a
> conservative lower bound that isolates the batching effect itself. Absolute figures vary with
> hardware, JVM warmup, and Docker overhead.

---

## 📊 Benchmark: actor dispatch throughput (firehose)

`DispatcherComparisonBenchmarkTest` measures end-to-end processing of a burst of 15,000,000
messages to a single actor, with the virtual-thread and the spinning dispatcher:

| Upstream Producers | bufferfly-core<br>*(Virtual Threads)* | bufferfly-cpu-bound<br>*(Hardware-Pinned Core)* | Speedup |
| :--- | :---: | :---: | :---: |
| **1 producer**   | ~5.5M msg/s | **⚡ 14.9M msg/s** | **2.71×** |
| **10 producers** | ~3.3M msg/s | **⚡ 13.9M msg/s** | **4.17×** |
| **50 producers** | ~2.8M msg/s | **⚡ 11.0M msg/s** | **3.97×** |

> A single producer gets closest to **15M msg/s** on the spinning engine, because no other
> producer contends for the mailbox's cache line.

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

## Run them yourself

Requires Docker to be running (the database and Kafka tests skip cleanly without it):

```bash
./gradlew :bufferfly-benchmarks:benchmarks --tests '*PersistenceActorBenchmarkTest*'
./gradlew :bufferfly-benchmarks:benchmarks --tests '*DispatcherComparisonBenchmarkTest*'
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
