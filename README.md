# 🦋 BufferFly

BufferFly is a ultra-high-throughput, lightweight, **Virtual-Thread-native Actor framework** for Java 25+. It is designed specifically to act as an elastic 
in-memory shock absorber and natural batching engine in front of high-volume persistent databases, driven directly by **Apache Kafka** partition streams.

By combining the **Single-Writer Principle** with Java 25 Virtual Threads and local non-spinning concurrent queues, 
BufferFly eliminates database row-lock contention and small I/O operations, 
transforming your transactional layer into a highly efficient bulk-ingestion warehouse.

---

## 🚀 Key Architectural Advantages

* **Zero Row-Level DB Locking:** Leverages distributed hashing to map one Kafka partition to exactly one Virtual Thread Actor. Concurrency is handled entirely in-memory—eliminating `enq: TX - row lock contention` waits.
* **Separation of Concerns:** Deeply splits your architecture into an **Actor Processing Loop** (pure memory, rules, and state decisions) and a **Persistence Runner Loop** (blocking database network I/O) running on isolated virtual thread pools.
* **Timeout-Based Natural Batching:** Dynamically groups writes greedily based on system load (e.g., flush every 1,000 records OR 50 milliseconds, whichever comes first). Maximize bulk updates (`MERGE`) at peak load while ensuring sub-millisecond fresh data under trickle loads.
* **Fencing Against Zombies:** Protects against cluster split-brain/rebalance pauses by tracking Kafka offsets directly inside your database transactions, utilizing optimistic lock verification to instantly terminate rogue nodes.
* **Elastic Backpressure Hooks:** Automatically pauses and resumes Spring Kafka listener containers via high/low queue watermarks, preventing `OutOfMemory` crashes without throwing thread stalls or forcing unrecoverable crash loops.

---

## 📐 The Processing Pipeline



### 2. Configure Natural Batching & Backpressure
Configure the behavior of your data absorption layers inside your `application.yml`:

```yaml
bufferfly:
  actors:
    pool-name: transaction-actor-pool
    high-watermark: 40000    # Pause Kafka if persistence buffer hits this limit
    low-watermark: 5000      # Resume Kafka once buffer drains down here
  persistence:
    batch-size: 1000         # Maximum rows per array write
    timeout-ms: 50           # Max wait time for a batch to fill under light load
```

---

## 🔒 Production Tuning & JVM Requirements

To maintain peak throughput and avoid carrier thread pinning when linking Java 25 Virtual Threads to relational database drivers, apply the following production configurations:

1. **Oracle JDBC Driver:** Ensure you are utilizing **Oracle JDBC 23c or higher**. These versions replace old internal object monitors with `ReentrantLock`, allowing persistence virtual threads to smoothly unmount from carrier threads during long I/O operations.
2. **JVM Performance Flags:** Run your cluster nodes using Project Lilliput optimizations to compress memory headers by up to 20%, saving room for your in-memory actor buffers:

```bash
java -Xms5g -Xmx5g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=250 \
     -XX:+AlwaysPreTouch \
     -XX:+UseCompactObjectHeaders \
     -Djdk.tracePinnedThreads=short \
     -jar app.jar
```

---

## 🗺️ Open Source Roadmap

BufferFly is an actively growing ecosystem built to revolutionize high-throughput enterprise systems. The core development milestones include:
- [x] Separate CPU-bound Actor & I/O-bound Persistence Runner Loops
- [ ] Functional `Container::pause` Backpressure Bridge
- [ ] Optimistic Lock Fencing via Database Co-Persistence Trans-Tracking
- [ ] Complete Declarative Hierarchical Actor Topology (`ActorContext`)
- [ ] Out-of-the-box cluster dashboards for monitoring processing lag vs DB flush latency

---

## 📄 License

BufferFly is open-source software licensed under the [Apache License, Version 2.0](LICENSE).

