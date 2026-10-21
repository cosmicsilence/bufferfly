package io.bufferfly.spring;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
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

    /**
     * Runs actor stop+start restarts off the caller thread, so an
     * {@code onUnrecoverable} hook (which fires on the persistence thread) does
     * not restart its own actor inline. Single-threaded: restarts are serialized.
     */
    private final java.util.concurrent.ExecutorService restartExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "bufferfly-sharded-restart");
                t.setDaemon(true);
                return t;
            });

    /** Per-partition runtime state, created on assign, removed on revoke. */
    private final Map<TopicPartition, Shard<V>> shards = new ConcurrentHashMap<>();

    /** Per-partition seek state (callback + buffered request). */
    private final Map<TopicPartition, SeekState> seekStates = new ConcurrentHashMap<>();

    /**
     * How a partition is paused/resumed. Default to the live container's
     * {@code pausePartition}/{@code resumePartition}; overridable in tests where
     * no broker is running.
     */
    private volatile java.util.function.Consumer<TopicPartition> pausePartitionAction;
    private volatile java.util.function.Consumer<TopicPartition> resumePartitionAction;

    private record Shard<V>(ActorReference<ConsumedRecord<V>> actorRef, ShardedActorConsumer<V> consumer) {
    }

    public ShardedActorContainer(String name,
                                 String topic,
                                 int concurrency,
                                 ShardedActorFactory<V> factory,
                                 ConsumerFactory<K, V> consumerFactory,
                                 ActorManager actorManager) {
        this.name = Objects.requireNonNull(name, "name");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.actorManager = Objects.requireNonNull(actorManager, "actorManager");
        Objects.requireNonNull(consumerFactory, "consumerFactory");
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");

        ContainerProperties props = new ContainerProperties(topic);
        props.setGroupId(name);
        props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Fire onIdleContainer periodically so a seek signalled by an actor's
        // asynchronous preStart is flushed even when no records are arriving.
        props.setIdleEventInterval(200L);
        props.setMessageListener(new PartitionRoutingListener());
        props.setConsumerRebalanceListener(new ShardRebalanceListener());

        this.container = new ConcurrentMessageListenerContainer<>(consumerFactory, props);
        this.container.setConcurrency(concurrency);
        this.container.setBeanName("bufferfly-sharded-" + name);
        this.pausePartitionAction = this.container::pausePartition;
        this.resumePartitionAction = this.container::resumePartition;
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
        shards.keySet().forEach(this::stopShard);
        shards.clear();
        seekStates.clear();
        restartExecutor.shutdownNow();
    }

    public ConcurrentMessageListenerContainer<K, V> container() {
        return container;
    }

    public String name() {
        return name;
    }

    // -- inspection helpers -------------------------------------------------

    /** @return {@code true} if an actor shard currently owns the given partition. */
    public boolean hasShard(TopicPartition tp) {
        return shards.containsKey(tp);
    }

    /** @return the number of partitions currently assigned and running an actor. */
    public int shardCount() {
        return shards.size();
    }

    // -- lifecycle ----------------------------------------------------------

    private void startShard(TopicPartition tp) {
        shards.computeIfAbsent(tp, partition -> {
            SeekState seekState = seekStates.computeIfAbsent(partition, p -> new SeekState());
            KafkaContextImpl ctx = new KafkaContextImpl(partition, seekState);
            Actor<ConsumedRecord<V>> actor = factory.createActor(partition, ctx);
            ActorReference<ConsumedRecord<V>> ref = actorManager.start(actor);
            ShardedActorConsumer<V> consumerBridge =
                    new ShardedActorConsumer<>(partition, ref, this::pausePartition);
            log.info("Assigned partition {} -> started actor '{}'", partition, actor.name());
            return new Shard<>(ref, consumerBridge);
        });
    }

    private void stopShard(TopicPartition tp) {
        Shard<V> shard = shards.remove(tp);
        // NOTE: do NOT touch seekStates here. The ConsumerSeekCallback is bound
        // to the Kafka *assignment*, which survives an internal actor restart
        // (stopShard + startShard without a rebalance). The SeekState is cleared
        // only on a genuine revoke/lost (see ShardRebalanceListener /
        // PartitionRoutingListener.onPartitionsRevoked) or on container stop().
        if (shard != null) {
            log.info("Stopping actor for partition {}", tp);
            shard.actorRef().stop();
        }
    }

    private void pausePartition(TopicPartition tp) {
        pausePartitionAction.accept(tp);
    }

    private void resumePartition(TopicPartition tp) {
        resumePartitionAction.accept(tp);
    }

    /**
     * Stops and re-starts the actor owning {@code tp}, forcing its new instance
     * to re-seek to the DB-stored offset. Runs asynchronously on
     * {@link #restartExecutor} so an {@code onUnrecoverable} hook firing on the
     * persistence thread does not restart its own actor inline.
     */
    private void restartShard(TopicPartition tp) {
        restartExecutor.execute(() -> {
            if (!shards.containsKey(tp)) {
                return; // partition no longer owned here (e.g. revoked meanwhile)
            }
            log.warn("Restarting actor for {} (unrecoverable failure) -> rewinding to stored offset", tp);
            stopShard(tp);
            startShard(tp);            // new preStart() re-seeks to the DB offset
            resumePartition(tp);       // in case the partition was paused before the restart
        });
    }

    /** Test seam: override how partitions are paused (no live container required). */
    void setPausePartitionAction(java.util.function.Consumer<TopicPartition> action) {
        this.pausePartitionAction = Objects.requireNonNull(action, "action");
    }

    /** Test seam: override how partitions are resumed (no live container required). */
    void setResumePartitionAction(java.util.function.Consumer<TopicPartition> action) {
        this.resumePartitionAction = Objects.requireNonNull(action, "action");
    }

    // -- message listener (also ConsumerSeekAware for signal-based seeks) ---

    private final class PartitionRoutingListener
            implements AcknowledgingConsumerAwareMessageListener<K, V>, ConsumerSeekAware {

        @Override
        public void onMessage(ConsumerRecord<K, V> record, Acknowledgment ack, Consumer<?, ?> consumer) {
            onRecord(record, ack);
        }

        @Override
        public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            // Runs on the consumer thread. Give each assigned partition's
            // SeekState the callback and flush any seek the actor already
            // requested during its (possibly async) preStart.
            assignments.keySet().forEach(tp -> {
                SeekState state = seekStates.computeIfAbsent(tp, p -> new SeekState());
                state.bind(callback);
            });
        }

        @Override
        public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            // Runs on the consumer thread. Covers the race where the actor's
            // async preStart signalled a seek AFTER onPartitionsAssigned fired
            // and before any record arrived: flush it now.
            assignments.keySet().forEach(tp -> {
                SeekState state = seekStates.get(tp);
                if (state != null) {
                    state.bind(callback);
                }
            });
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Genuine revoke: the assignment (and thus the seek callback) is gone.
            // Drop the SeekState entirely. (An internal actor restart does NOT go
            // through here, so a restarted actor keeps its bound callback.)
            partitions.forEach(seekStates::remove);
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
        // Consumer thread: apply any seek the actor signalled (e.g. during
        // preStart) before we hand this record over.
        SeekState seekState = seekStates.get(tp);
        if (seekState != null) {
            seekState.flushPending();
            // Discard records left over from a poll taken BEFORE a rewind-seek
            // took effect. Without this, a stale record (e.g. a higher offset
            // fetched pre-seek) could reach the actor out of order and advance
            // persisted state past records that were never processed.
            if (!seekState.shouldDeliver(record.offset())) {
                log.debug("Discarding stale pre-seek record {} at offset {}", tp, record.offset());
                return false;
            }
        }
        Shard<V> shard = shards.get(tp);
        if (shard == null) {
            // Partition not (yet) owned here — do not ack; it will be
            // redelivered after the shard is (re)assigned.
            log.warn("No shard for {} at offset {}; skipping (will redeliver)", tp, record.offset());
            return false;
        }
        shard.consumer().tellActor(record, ack);
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
        partitions.forEach(this::startShard);
    }

    /** Package-private seam: revoke partitions (stop each shard). */
    void onRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(this::stopShard);
    }

    /** bind a seek callback to a partition as the consumer thread would. */
    void bindSeekCallback(TopicPartition tp, ConsumerSeekAware.ConsumerSeekCallback cb) {
        seekStates.computeIfAbsent(tp, p -> new SeekState()).bind(cb);
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
            pending = cb -> cb.seek(tp.topic(), tp.partition(), offset);
            flushIfOnConsumerThreadImpossible();
        }

        synchronized void signalSeekToBeginning(TopicPartition tp) {
            expectedNextOffset = 0;
            pending = cb -> cb.seekToBeginning(tp.topic(), tp.partition());
            flushIfOnConsumerThreadImpossible();
        }

        synchronized void signalSeekToEnd(TopicPartition tp) {
            expectedNextOffset = -1; // cannot predict the end offset; accept what arrives
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
