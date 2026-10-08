
🏎️ BufferFly: Ultra-High-Performance Actor Infrastructure for Java

BufferFly is a next-generation actor framework designed to push the absolute physical boundaries of execution efficiency on modern hardware.
The framework is divided into two discrete components, isolating standard high-concurrency patterns from specialized low-latency computing:

• bufferfly-cpu-bound: A zero-overhead execution environment configured specifically for pinning hot execution paths to physical CPU hardware using a customized spinning dispatcher layout.

🔥 Breaking the 15 Million Messages/Sec Mark

During head-to-head microbenchmarking simulating intense, concurrent multi-producer firehose pressure, BufferFly shatters execution expectations for high-performance computing in managed languages. By aligning software constructs directly with processor architectures (Mechanical Sympathy), it peaks at over 15,000,000 messages per second delivered to a single actor.
================================================================================
 BENCHMARK — HARDWARE-PINNED FIREHOSE PERFORMANCE (5,000,000 Messages)
--------------------------------------------------------------------------------
 Upstream Producers     VTDispatcher (Virtual Threads)   BufferFly (Pinned XADD Core)
--------------------------------------------------------------------------------
 1 Active Producer                 ~5.5M msg/s                 ⚡ 16.08M msg/s
 10 Contending Producers           ~4.4M msg/s                 ⚡ 12.61M msg/s
 50 Contending Producers           ~4.6M msg/s                 ⚡ 12.50M msg/s
================================================================================

Why standard architectures fall off a cliff:

Traditional low-latency architectures using standard Multi-Producer Single-Consumer (MPSC) arrays hit a definitive concurrency inversion cliff—collapsing to less than 2.1M msg/s at 50 producers. This happens because competing threads get caught in heavy Compare-And-Swap (LOCK CMPXCHG) retry loops, constantly thrashing the CPU cache coherency protocol (MESI).
Virtual threads avoid this thrashing by parking threads via a LinkedBlockingQueue layout, but remain locked to a definitive throughput ceiling (~4.6M msg/s) due to continuation allocations, stack frame tracking, and ForkJoinPool carrier scheduling overhead.

🧠 The Engineering Under the Hood

To maintain a flat, unyielding 12.5M+ msg/s line even under a massive storm of 50 concurrent writers, bufferfly-cpu-bound utilizes a highly tailored hardware-level optimization loop:
[Upstream Producers] ======( Hardware LOCK XADD )======> [ JCTools MpscUnboundedXaddArrayQueue ]
                                                                       ║
                                                            (Zero Cache-Line Bouncing)
                                                                       ▼
                                                          [ SpinningActorDispatcher ]
                                                          📌 Pinned OS Thread (100% Core)
                                                          🔄 Infinite Busy-Spin Loop
                                                          ⏱️ 1-in-65,536 Bitwise Self-Healing

1. Hardware Fetch-And-Add (LOCK XADD)

Instead of standard CAS retry loops, BufferFly uses JCTools' MpscUnboundedXaddArrayQueue. Upstream threads reserve their array indices in a single instruction cycle directly at the hardware layer. There are zero software retries, eliminating cache line bouncing and allowing your producers to dispatch work and immediately return.

2. Core-Pinned Infinite Busy-Spins

The SpinningActorDispatcher bypasses traditional kernel scheduling, thread parking, and waking states. It allocates a dedicated OS platform thread that sits in a permanent, tight busy-spin loop. 
The execution pipeline is kept 100% warm within the assigned L1/L2 cache of a single physical core.