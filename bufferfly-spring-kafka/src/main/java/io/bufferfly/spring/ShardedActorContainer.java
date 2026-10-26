package io.bufferfly.spring;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.metrics.BufferflyMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wires a single {@link ShardedActor}-annotated factory to a Spring Kafka
 * {@link ConcurrentMessageListenerContainer}, running <b>one BufferFly actor per
 * partition</b>.
 *
 * <h2>Lifecycle (rebalance-driven)</h2>
 * <ul>
 *   <li><b>onPartitionsAssigned</b> → for each newly assigned partition, build a
 *       {@link KafkaContext}, ask the {@link ShardedActorFactory} to create the
 *       actor, start it via the {@link ActorManager}, and register a
 *       {@link ShardedActorConsumer}. The actor typically seeks back to its last
 *       DB-stored offset during {@code preStart()} using the context.</li>
 *   <li><b>onPartitionsRevoked</b> → stop the actor for each revoked partition
 *       and drop its consumer, so a different instance can take over cleanly.</li>
 * </ul>
 *
 * <h2>No message loss</h2>
 * The container uses {@code MANUAL_IMMEDIATE} ack. The {@link ShardedActorConsumer}
 * acknowledges a record only after the actor accepted it. Records rejected under
 * backpressure are left un-acked and redelivered after resume.
 *
 * <h2>Seeking (signal-based)</h2>
 * Kafka's {@code Consumer} is single-threaded and must never be touched from an
 * actor's (virtual) thread. Seeks are therefore <em>signals</em>: a
 * {@link KafkaContext#seek(long)} call records the request and hands it to
 * Spring's {@link ConsumerSeekAware.ConsumerSeekCallback}, which the container
 * applies on the consumer thread before the next poll. Requests made before the
 * callback is available (e.g. during an actor's asynchronous {@code preStart})
 * are buffered and flushed once the partition's callback arrives.
 *
 * <h2>Backpressure</h2>
 * Two cooperating mechanisms:
 * <ul>
 *   <li><b>Persistence-driven (proactive):</b> the actor's persistence layer,
 *       when its buffer hits the configured high watermark, calls
 *       {@link KafkaContext#pausePartition()} which pauses only this actor's
 *       partition; at the low watermark it calls
 *       {@link KafkaContext#resumePartition()}.</li>
 *   <li><b>Per-record (reactive safety net):</b> if a {@code tell} throws
 *       {@code UnderPressureException} or {@code MailBoxRefusedException}, the
 *       consumer pauses just that partition and leaves the record un-acked for
 *       redelivery.</li>
 * </ul>
 *
 * @param <K> Kafka key type
 * @param <V> Kafka value type
 */
public final class ShardedActorContainer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ShardedActorContainer.class);

    private final String name;
    private final String topic;
    private final ShardedActorFactory<V> factory;
    private final ActorManager actorManager;
    private final ConcurrentMessageListenerContainer<K, V> container;

    /** Observability sink; {@link BufferflyMetrics#NOOP} when none wired. */
    private final BufferflyMetrics metrics;

    /**
     * The message listener / lifecycle component. It owns the per-partition
     * {@link Shard}s and the single {@link RestartSupervisor} actor reference —
     * everything tied to routing records and the assign/revoke/restart lifecycle
     * lives here, co-located with the Kafka callbacks that drive it.
     */
    private final PartitionRoutingListener routing = new PartitionRoutingListener();

    /**
     * How a partition is paused/resumed. Default to the live container's
     * {@code pausePartition}/{@code resumePartition}; overridable in tests where
     * no broker is running.
     */
    private volatile java.util.function.Consumer<TopicPartition> pausePartitionAction;
    private volatile java.util.function.Consumer<TopicPartition> resumePartitionAction;

    /**
     * All state belonging to one assigned partition, co-located and sharing the
     * partition's <b>assignment lifetime</b>: created on assign, removed on a
     * genuine revoke. The {@link #seekState} is stable for the whole assignment
     * (it holds the bound {@code ConsumerSeekCallback} and the rewind gate, which
     * must survive an internal actor restart); the {@link #actorRef} and
     * {@link #consumer} are <b>swapped</b> on each restart (they are {@code null}
     * in the brief window between stop and the fresh actor starting).
     */
    private static final class Shard<V> {
        final SeekState seekState = new SeekState();
        volatile ActorReference<ConsumedRecord<V>> actorRef;
        volatile ShardedActorConsumer<V> consumer;

        boolean hasActor() {
            return actorRef != null;
        }
    }

    public ShardedActorContainer(String name,
                                 String topic,
                                 int concurrency,
                                 ShardedActorFactory<V> factory,
                                 ConsumerFactory<K, V> consumerFactory,
                                 ActorManager actorManager) {
        this(name, topic, concurrency, factory, consumerFactory, actorManager, BufferflyMetrics.NOOP);
    }

    public ShardedActorContainer(String name,
                                 String topic,
                                 int concurrency,
                                 ShardedActorFactory<V> factory,
                                 ConsumerFactory<K, V> consumerFactory,
                                 ActorManager actorManager,
                                 BufferflyMetrics metrics) {
        this.name = Objects.requireNonNull(name, "name");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.actorManager = Objects.requireNonNull(actorManager, "actorManager");
        this.metrics = metrics == null ? BufferflyMetrics.NOOP : metrics;
        Objects.requireNonNull(consumerFactory, "consumerFactory");
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");

        ContainerProperties props = new ContainerProperties(topic);
        props.setGroupId(name);
        props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Fire onIdleContainer periodically so a seek signalled by an actor's
        // asynchronous preStart is flushed even when no records are arriving.
        props.setIdleEventInterval(200L);
        props.setMessageListener(routing);
        props.setConsumerRebalanceListener(new ShardRebalanceListener());

        this.container = new ConcurrentMessageListenerContainer<>(consumerFactory, props);
        this.container.setConcurrency(concurrency);
        this.container.setBeanName("bufferfly-sharded-" + name);
        this.pausePartitionAction = this.container::pausePartition;
        this.resumePartitionAction = this.container::resumePartition;

        // Start the restart supervisor actor eagerly and hand it to the routing
        // component, which owns the restart/lifecycle path. Ready before any
        // partition is assigned (including in seam-driven tests).
        routing.supervisorRef = actorManager.start(
                new RestartSupervisor("bufferfly-restart-supervisor-" + name, new SupervisorActions()));
    }

    /** Starts the underlying Kafka listener container. */
    public void start() {
        log.info("Starting sharded actor container name={} topic={} concurrency={}",
                name, topic, container.getConcurrency());
        container.start();
    }

    /** Stops the container and all per-partition actors. */
    public void stop() {
        container.stop();
        routing.releaseAll();
        ActorReference<RestartSupervisor.SupervisorMsg> sup = routing.supervisorRef;
        if (sup != null) {
            sup.stop();
            routing.supervisorRef = null;
        }
    }

    public ConcurrentMessageListenerContainer<K, V> container() {
        return container;
    }

    /**
     * @return whether the underlying Kafka listener container is running (its
     *         consumer thread(s) are active and polling). This is the single
     *         liveness fact a pod restart can fix — see {@code BufferflyHealthIndicator}.
     */
    public boolean isRunning() {
        return container.isRunning();
    }

    public String name() {
        return name;
    }

    // -- inspection helpers -------------------------------------------------

    /** @return {@code true} if an actor shard currently owns the given partition. */
    public boolean hasShard(TopicPartition tp) {
        return routing.hasShard(tp);
    }

    /** @return the number of partitions currently assigned and running an actor. */
    public int shardCount() {
        return routing.shardCount();
    }

    // -- lifecycle ----------------------------------------------------------

    // -- lifecycle ----------------------------------------------------------

    /**
     * Creates and starts a fresh actor for {@code shard}'s partition, reusing the
     * shard's stable {@link SeekState} (so a restart keeps the bound seek
     * callback and rewind gate). Sets the shard's {@code actorRef}/{@code consumer}.
     */
    private void startActor(TopicPartition tp, Shard<V> shard) {
        KafkaContextImpl ctx = new KafkaContextImpl(tp, shard.seekState);
        Actor<ConsumedRecord<V>> actor = factory.createActor(tp, ctx);
        ActorReference<ConsumedRecord<V>> ref = actorManager.start(actor);
        shard.actorRef = ref;
        shard.consumer = new ShardedActorConsumer<>(tp, ref, this::pausePartition, metrics, factory.actorName(tp));
        log.info("Partition {} -> started actor '{}'", tp, actor.name());
    }

    private void pausePartition(TopicPartition tp) {
        pausePartitionAction.accept(tp);
        metrics.partitionPaused(factory.actorName(tp));
    }

    private void resumePartition(TopicPartition tp) {
        resumePartitionAction.accept(tp);
        metrics.partitionResumed(factory.actorName(tp));
    }

    /**
     * Requests a supervised restart of the actor owning {@code tp}. Delegates to
     * the {@link RestartSupervisor} actor (held by {@link #routing}), which owns
     * the per-partition backoff state and sequences the
     * pause → stop → wait → start+rewind → resume flow on its own thread. Safe to
     * call from any thread (e.g. a persistence-thread {@code onUnrecoverable}
     * hook): it is just a {@code tell}.
     */
    private void restartShard(TopicPartition tp) {
        routing.requestRestart(tp);
    }

    /**
     * The concrete restart steps the {@link RestartSupervisor} drives. Each is a
     * thin call into the container/routing mechanics; all policy/state/timing
     * lives in the supervisor actor.
     */
    private final class SupervisorActions implements RestartSupervisor.RestartActions {
        @Override
        public void pause(TopicPartition tp) {
            pausePartition(tp);
        }

        @Override
        public void beginRewind(TopicPartition tp) {
            // Arm the rewind gate so stale pre-restart records are discarded
            // until the fresh actor rewinds.
            Shard<V> shard = routing.shard(tp);
            if (shard != null) {
                shard.seekState.beginRewind();
            }
        }

        @Override
        public void stopShard(TopicPartition tp) {
            routing.stopActorForRestart(tp);
        }

        @Override
        public void startShard(TopicPartition tp) {
            routing.startActorForRestart(tp);
        }

        @Override
        public void resume(TopicPartition tp) {
            resumePartition(tp);
        }

        @Override
        public boolean owns(TopicPartition tp) {
            // The Shard survives an internal restart and is removed only on a
            // genuine revoke / container stop, so its presence is the ownership
            // signal (even in the window where actorRef is momentarily null).
            return routing.hasShard(tp);
        }
    }

    /** Test seam: override how partitions are paused (no live container required). */
    void setPausePartitionAction(java.util.function.Consumer<TopicPartition> action) {
        this.pausePartitionAction = Objects.requireNonNull(action, "action");
    }

    /** Test seam: override how partitions are resumed (no live container required). */
    void setResumePartitionAction(java.util.function.Consumer<TopicPartition> action) {
        this.resumePartitionAction = Objects.requireNonNull(action, "action");
    }

    // -- message listener + lifecycle owner (shards + supervisor) -----------

    /**
     * Owns everything tied to routing and the partition lifecycle: the
     * per-partition {@link Shard}s and the {@link RestartSupervisor} reference.
     * It is both the Kafka {@code MessageListener} and {@code ConsumerSeekAware}
     * callback target, so record routing, seek relaying, and assign/revoke all
     * sit next to the state they touch.
     */
    private final class PartitionRoutingListener
            implements AcknowledgingConsumerAwareMessageListener<K, V>, ConsumerSeekAware {

        /** Per-partition state, created on assign, removed on genuine revoke. */
        private final Map<TopicPartition, Shard<V>> shards = new ConcurrentHashMap<>();

        /** The single restart supervisor actor for this container. */
        private volatile ActorReference<RestartSupervisor.SupervisorMsg> supervisorRef;

        // -- shard registry access (used by the container/supervisor actions) --

        Shard<V> shard(TopicPartition tp) {
            return shards.get(tp);
        }

        boolean hasShard(TopicPartition tp) {
            return shards.containsKey(tp);
        }

        int shardCount() {
            return shards.size();
        }

        /** Assign: create the shard (with its stable SeekState) and start its actor. */
        void assign(TopicPartition tp) {
            Shard<V> shard = shards.computeIfAbsent(tp, p -> new Shard<>());
            if (!shard.hasActor()) {
                ShardedActorContainer.this.startActor(tp, shard);
                metrics.partitionAssigned(factory.actorName(tp));
            }
        }

        /** Start a fresh actor for an existing shard (restart path). */
        void startActorForRestart(TopicPartition tp) {
            Shard<V> shard = shards.get(tp);
            if (shard != null && !shard.hasActor()) {
                ShardedActorContainer.this.startActor(tp, shard);
                metrics.actorRestarted(factory.actorName(tp));
            }
        }

        /** Stop the actor but KEEP the shard + its SeekState (restart path). */
        void stopActorForRestart(TopicPartition tp) {
            Shard<V> shard = shards.get(tp);
            if (shard != null && shard.actorRef != null) {
                log.info("Stopping actor for partition {} (restart)", tp);
                shard.actorRef.stop();
                shard.actorRef = null;
                shard.consumer = null;
            }
        }

        /** Genuine revoke/stop: stop the actor AND drop the shard (and its SeekState). */
        void releaseShard(TopicPartition tp) {
            Shard<V> shard = shards.remove(tp);
            if (shard != null && shard.actorRef != null) {
                log.info("Releasing partition {} (revoke)", tp);
                shard.actorRef.stop();
                metrics.partitionRevoked(factory.actorName(tp));
            }
        }

        void releaseAll() {
            shards.keySet().forEach(this::releaseShard);
        }

        /** Ask the supervisor to restart the partition's actor (best-effort tell). */
        void requestRestart(TopicPartition tp) {
            ActorReference<RestartSupervisor.SupervisorMsg> sup = supervisorRef;
            if (sup == null) return;
            try {
                sup.tell(new RestartSupervisor.RestartRequested(tp));
            } catch (RuntimeException e) {
                log.debug("Restart request for {} dropped: supervisor unavailable ({})", tp, e.toString());
            }
        }

        // -- Kafka callbacks ------------------------------------------------

        @Override
        public void onMessage(ConsumerRecord<K, V> record, Acknowledgment ack, Consumer<?, ?> consumer) {
            onRecord(record, ack);
        }

        @Override
        public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            // Runs on the consumer thread. Bind the callback to each partition's
            // (stable) SeekState and flush any seek the actor already signalled
            // during its (async) preStart.
            assignments.keySet().forEach(tp -> {
                Shard<V> shard = shards.computeIfAbsent(tp, p -> new Shard<>());
                shard.seekState.bind(callback);
            });
        }

        @Override
        public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            // Covers the race where the actor's async preStart signalled a seek
            // AFTER onPartitionsAssigned and before any record arrived.
            assignments.keySet().forEach(tp -> {
                Shard<V> shard = shards.get(tp);
                if (shard != null) {
                    shard.seekState.bind(callback);
                }
            });
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Genuine revoke: drop the shard entirely (its SeekState + bound
            // callback go with it), and tell the supervisor to forget the
            // partition's restart ramp so a later reassignment starts fresh.
            ActorReference<RestartSupervisor.SupervisorMsg> sup = supervisorRef;
            partitions.forEach(tp -> {
                releaseShard(tp);
                if (sup != null) {
                    try {
                        sup.tell(new RestartSupervisor.PartitionReleased(tp));
                    } catch (RuntimeException ignored) {
                        // supervisor stopped concurrently — nothing to forget
                    }
                }
            });
        }
    }

    /**
     * Routes one record to its partition's shard. Package-private seam exercised
     * directly by both the live message listener and unit tests.
     *
     * @return {@code true} if a shard handled the record; {@code false} if no
     *         shard owns the partition (record left un-acked for redelivery)
     */
    boolean onRecord(ConsumerRecord<K, V> record, Acknowledgment ack) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Shard<V> shard = routing.shard(tp);
        if (shard != null) {
            // Consumer thread: apply any seek the actor signalled (e.g. during
            // preStart) before we hand this record over.
            shard.seekState.flushPending();
            // Discard records left over from a poll taken BEFORE a rewind-seek
            // took effect, or any record while a restart's rewind is pending.
            if (!shard.seekState.shouldDeliver(record.offset())) {
                log.debug("Discarding stale/pre-rewind record {} at offset {}", tp, record.offset());
                metrics.recordRejected(factory.actorName(tp));
                return false;
            }
        }
        if (shard == null || !shard.hasActor()) {
            // Partition not owned here, or its actor is momentarily down during a
            // restart — do not ack; it will be redelivered.
            log.warn("No active actor for {} at offset {}; skipping (will redeliver)", tp, record.offset());
            metrics.recordRejected(factory.actorName(tp));
            return false;
        }
        shard.consumer.tellActor(record, ack);
        return true;
    }

    // -- rebalance listener -------------------------------------------------

    private final class ShardRebalanceListener implements ConsumerAwareRebalanceListener {

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            onAssigned(partitions);
        }

        @Override
        public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            // Stop actors before offsets are committed so no further processing
            // happens on partitions we are losing.
            onRevoked(partitions);
        }

        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            onRevoked(partitions);
        }
    }

    /** Package-private seam: assign partitions (start a shard each). */
    void onAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(routing::assign);
    }

    /** Package-private seam: revoke partitions (release each shard). */
    void onRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(routing::releaseShard);
    }

    /** bind a seek callback to a partition as the consumer thread would. */
    void bindSeekCallback(TopicPartition tp, ConsumerSeekAware.ConsumerSeekCallback cb) {
        Shard<V> shard = routing.shards.computeIfAbsent(tp, p -> new Shard<>());
        shard.seekState.bind(cb);
    }

    // -- seek state ---------------------------------------------------------

    /**
     * Thread-safe mailbox between an actor (which <em>signals</em> a seek from
     * its own thread) and the Kafka consumer thread (which owns the
     * {@link ConsumerSeekAware.ConsumerSeekCallback} and actually performs it).
     *
     * <p>A seek requested before the callback is bound is buffered as
     * {@code pending} and flushed when {@link #bind} is called.
     */
    private static final class SeekState {
        private ConsumerSeekAware.ConsumerSeekCallback callback;
        private java.util.function.Consumer<ConsumerSeekAware.ConsumerSeekCallback> pending; // buffered seek

        /**
         * The offset the next delivered record is expected to have. Set when a
         * seek is signalled. {@code -1} means "no expectation" (accept whatever
         * arrives — e.g. before any seek, or after seekToEnd). Used to DISCARD
         * records left over from a poll that happened before a rewind-seek took
         * effect, so a stale higher/earlier offset can never slip through.
         */
        private long expectedNextOffset = -1;

        /**
         * {@code true} while a restart is in progress: the old actor has been
         * torn down and a fresh one is being started, but its asynchronous
         * {@code preStart} has not yet recorded the rewind-seek. While set, ALL
         * records for this partition are discarded (not delivered and not acked),
         * so a stale record left over from the pre-restart poll position can
         * never reach the new actor before it has rewound. Cleared the moment the
         * new actor signals its seek (see {@link #signalSeek},
         * {@link #signalSeekToBeginning}, {@link #signalSeekToEnd}).
         *
         * <p>This closes the exactly-once window where a brand-new shard is
         * already registered (so {@code onRecord} would route to it) but
         * {@code expectedNextOffset} is still {@code -1} ("accept anything"),
         * which would otherwise let a pre-rewind record be re-processed.
         */
        private boolean awaitingRewind = false;

        /**
         * Called on the restart-executor thread, synchronously, BEFORE the new
         * actor is started. Puts the partition into the "discard everything until
         * the new actor rewinds" state described on {@link #awaitingRewind}.
         */
        synchronized void beginRewind() {
            awaitingRewind = true;
        }

        synchronized void bind(ConsumerSeekAware.ConsumerSeekCallback cb) {
            this.callback = cb;
            flushPending();
        }

        /**
         * Decides whether a just-polled record should be delivered to the actor.
         * Runs on the consumer thread (from {@code onRecord}).
         *
         * <ul>
         *   <li>no expectation ({@code -1}) → deliver (steady state, no seek in
         *       effect);</li>
         *   <li>offset == expected → deliver and advance the expectation by one;</li>
         *   <li>any other offset (lower = stale leftover from a pre-seek poll, or
         *       higher = out-of-order ahead of the sought position) → discard.</li>
         * </ul>
         *
         * <p>After a rewind-seek Kafka re-delivers strictly in order from the
         * sought offset, so the expected sequence arrives contiguously; only
         * records left over from the previous poll fail the {@code == expected}
         * check and are dropped.
         */
        synchronized boolean shouldDeliver(long recordOffset) {
            if (awaitingRewind) {
                // Restart in flight: the new actor has not rewound yet. Discard
                // everything so no pre-rewind record slips through to it.
                return false;
            }
            if (expectedNextOffset < 0) {
                return true;
            }
            if (recordOffset == expectedNextOffset) {
                expectedNextOffset++;
                return true;
            }
            return false;
        }

        /**
         * Called on the <b>consumer thread</b> (from {@code onPartitionsAssigned},
         * {@code onRecord} or {@code onIdleContainer}). Applies any buffered seek
         * via the callback. The callback performs the actual
         * {@code KafkaConsumer} seek, so it must only ever run here — never on an
         * actor thread.
         */
        synchronized void flushPending() {
            if (callback != null && pending != null) {
                pending.accept(callback);
                pending = null;
            }
        }

        // The following are called from the ACTOR thread. They ONLY record the
        // intent; they never touch the callback / consumer. The consumer thread
        // later drains it via flushPending().

        synchronized void signalSeek(TopicPartition tp, long offset) {
            expectedNextOffset = offset;
            awaitingRewind = false; // the new actor has now recorded its rewind
            pending = cb -> cb.seek(tp.topic(), tp.partition(), offset);
            flushIfOnConsumerThreadImpossible();
        }

        synchronized void signalSeekToBeginning(TopicPartition tp) {
            expectedNextOffset = 0;
            awaitingRewind = false;
            pending = cb -> cb.seekToBeginning(tp.topic(), tp.partition());
            flushIfOnConsumerThreadImpossible();
        }

        synchronized void signalSeekToEnd(TopicPartition tp) {
            expectedNextOffset = -1; // cannot predict the end offset; accept what arrives
            awaitingRewind = false;
            pending = cb -> cb.seekToEnd(tp.topic(), tp.partition());
            flushIfOnConsumerThreadImpossible();
        }

        /**
         * Deliberately a no-op: a seek signalled from the actor thread is never
         * flushed here (that would run the callback off the consumer thread and
         * trip {@code KafkaConsumer}'s single-thread guard). It is flushed later
         * by the consumer thread. This method documents that intent.
         */
        private void flushIfOnConsumerThreadImpossible() {
            // no-op by design — see flushPending()
        }
    }

    // -- KafkaContext impl --------------------------------------------------

    /**
     * Per-partition {@link KafkaContext}. Seeks are <em>signals</em> relayed to
     * the consumer thread via {@link SeekState}; this object is safe to call from
     * the actor's own (virtual) thread and never touches the Kafka consumer
     * directly.
     */
    private final class KafkaContextImpl implements KafkaContext {
        private final TopicPartition tp;
        private final SeekState seekState;

        private KafkaContextImpl(TopicPartition tp, SeekState seekState) {
            this.tp = tp;
            this.seekState = seekState;
        }

        @Override
        public TopicPartition topicPartition() {
            return tp;
        }

        @Override
        public void seek(long offset) {
            if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, got " + offset);
            seekState.signalSeek(tp, offset);
        }

        @Override
        public void seekToBeginning() {
            seekState.signalSeekToBeginning(tp);
        }

        @Override
        public void seekToEnd() {
            seekState.signalSeekToEnd(tp);
        }

        @Override
        public void pausePartition() {
            ShardedActorContainer.this.pausePartition(tp);
        }

        @Override
        public void resumePartition() {
            ShardedActorContainer.this.resumePartition(tp);
        }

        @Override
        public void restartActor() {
            ShardedActorContainer.this.restartShard(tp);
        }
    }
}
