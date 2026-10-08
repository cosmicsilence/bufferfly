# 🦋 BufferFly

![Build Status](https://github.com/cosmicsilence/bufferfly/actions/workflows/build.yml/badge.svg)
[![Latest Version](https://img.shields.io/maven-central/v/io.github.cosmicsilence/bufferfly-core)](https://central.sonatype.com/artifact/io.github.cosmicsilence/bufferfly-core)
[![Java](https://img.shields.io/badge/Java-25+-orange.svg)](https://adoptium.net/temurin/releases/?version=25)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)




📦 bufferfly-core: Virtual-Thread-Native Actor Framework

🏎️ bufferfly-cpu-bound: Hardware-Pinned Core Dispatch Infrastructure

⚡ Overview & Framework Topology

BufferFly is a lightweight actor architecture for Java 25+, serving as an in-memory shock absorber and batching engine for Kafka streams protecting persistent databases. For complete architectural details, module topologies (bufferfly-core and bufferfly-cpu-bound), production tuning requirements, and configuration guidelines, please refer to the original source at GitHub Repository.

🚀 Performance & Module Selection Guide

BufferFly is explicitly engineered with strict mechanical sympathy. Depending on your workload requirements, 
you can opt for the highly elastic, zero-idle-cost virtual thread runtime or scale into the hardware-pinned spinning engine to break modern throughput barriers.
### 📊 Microbenchmark: High-Throughput Firehose Performance
*Measured end-to-end processing execution delivering a burst of 5,000,000 messages directly to a single target Actor path.*

| Upstream Producers | bufferfly-core <br>*(Virtual Threads)* | bufferfly-cpu-bound <br>*(Hardware-Pinned Core)* | Real-World Speedup |
| :--- | :---: | :---: | :---: |
| **1 Active Producer** | ~5.5M msg/s | **⚡ 14.25M msg/s** | **2.59x** |
| **10 Contending Producers** | ~4.8M msg/s | **⚡ 12.68M msg/s** | **2.64x** |
| **50 Contending Producers** | ~4.4M msg/s | **⚡ 13.82M msg/s** | **3.14x** |

> 📌 **Note:** Peak throughput configurations for single-producer environments frequently cross the **15,000,000 messages/sec mark** due to the total elimination of hardware cache-line invalidation cycles.

📦 bufferfly-core (The Virtual Thread Engine)

Designed for general-purpose, massive scaling where thousands of actors must co-exist seamlessly, and idle memory or CPU footprints must approach zero.
• The Blueprint: Employs a VTDispatcher paired with a specialized LinkedBlockingQueue configuration to hand off execution cycles directly to the JVM's underlying ForkJoinPool carrier threads.
• The Baseline: Caps out at a structural allocation ceiling of ~4.5M – 5.5M messages per second due to the natural overhead of virtual thread continuation tracking and stack frame processing.
• Best For: General business state machines, asynchronous web hook ingestion, and standard Kafka partition processing where extreme core saturation is not justified.

🏎️ bufferfly-cpu-bound (The Hardware-Pinned Spinning Engine)

Designed for microsecond-critical, ultra-high-frequency data ingestion where raw throughput limits must be shattered at the expense of an isolated physical hardware core.
• The 15 Million Message Mark: By combining a core-pinned infinite busy-spin thread (SpinningActorDispatcher) with a hardware-level Fetch-And-Add (LOCK XADD) mailbox (MpscUnboundedXaddArrayQueue), this module successfully eliminates the traditional "CAS retry cliff." It pushes performance to an outstanding 14.25M+ messages per second, touching a peak capability of over 15,000,000 messages per second on single-producer pipelines.
• Self-Healing Architecture: Features an amortized bitwise mask counter (& 0xFFFF) that evaluates core placement once every 65,536 cycles via a <0.5ns register operation. If the OS forces a thermal thread migration, the loop instantly snaps the thread back onto its original hardware alignment without introducing heavy JNI overhead.
• Best For: Financial order books, live market tickers, or high-volume Kafka partition streams with strict, sub-microsecond tail-latency SLAs.

## 📄 License

BufferFly is open-source software licensed under the [Apache License, Version 2.0](LICENSE).

