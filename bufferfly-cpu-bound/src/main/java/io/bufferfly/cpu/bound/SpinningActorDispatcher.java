package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.Actor;
import io.bufferfly.core.actor.BlockingMailbox;
import io.bufferfly.core.actor.Dispatcher;
import io.bufferfly.core.actor.Mailbox;
import net.openhft.affinity.AffinityLock;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ⚠️ CRITICAL PERFORMANCE WARNING — USE WITH EXTREME CAUTION ⚠️
 *
 * <p>The {@code SpinningActorDispatcher} allocates exactly one dedicated OS Platform
 * Thread per Actor instance. This thread utilizes aggressive, non-blocking polling
 * strategies (JCTools/Mechanical Sympathy) and will <b>busy-spin awake infinitely</b>
 * to completely eliminate thread scheduling and context-switching overhead.
 *
 * <h3>Hardware Implications:</h3>
 * <ul>
 *   <li><b>100% Core Saturation:</b> Each dispatched Actor will permanently pin a
 *       physical CPU core to 100% utilization, regardless of whether traffic is actively
 *       flowing through the mailbox.</li>
 *   <li><b>Thermal Throttling & Thread Migration:</b> Without native CPU core affinity,
 *       the OS kernel scheduler may forcibly migrate this spinning thread across cores
 *       to manage hardware heat spikes, inducing localized L1/L2 cache misses.</li>
 *   <li><b>Carrier Pool Starvation:</b> Never execute this dispatcher inside standard
 *       Java 25 Virtual Threads or ForkJoinPool contexts, as it will instantly hijack
 *       and starve the carrier threads.</li>
 * </ul>
 *
 * <p><b>Recommended Use Case:</b> Only utilize this dispatcher for a minimal, fixed
 * number of ultra-high-volume partition actors where processing latency requirements
 * are strictly deterministic (sub-microsecond) and the host machine has ample, isolated
 * physical cores.
 */
public class SpinningActorDispatcher implements Dispatcher {

    /**
     * Immutable configuration for a {@link SpinningActorDispatcher}.
     *
     * @param mailboxCapacity Maximum number of messages each actor's {@link MpscMailbox}
     *                        can hold before {@link #dispatch} throws
     *                        {@link IllegalStateException}. Must be {@code >= 1}.
     *                        Defaults to {@link MpscMailbox#DEFAULT_CAPACITY}.
     */
    public record Config(int mailboxCapacity) {
        public Config {
            if (mailboxCapacity < 1)
                throw new IllegalArgumentException(
                        "mailboxCapacity must be >= 1, got " + mailboxCapacity);
        }

        /** Returns a {@code Config} with {@link MpscMailbox#DEFAULT_CAPACITY}. */
        public static Config defaults() {
            return new Config(MpscMailbox.DEFAULT_CAPACITY);
        }
    }

    private final Config config;
    private final Map<String, SpinningThread<?>> cpuBoundThreads = new ConcurrentHashMap<>();
    private final Map<String, Mailbox<?>> mailboxes = new ConcurrentHashMap<>();

    /** Creates a dispatcher with {@link Config#defaults()}. */
    public SpinningActorDispatcher() {
        this(Config.defaults());
    }

    /** Creates a dispatcher with a custom {@link Config}. */
    public SpinningActorDispatcher(Config config) {
        this.config = config;
    }

    @Override
    public <T> void dispatch(T t, Actor<T> actor) {
        try {
            final var accepted = mailboxFor(actor).offer(t);
            if (!accepted) {
                throw new IllegalStateException(
                        "Mailbox refused for the actor.name=%s".formatted(actor.name()));
            }
        } finally {
            consume(actor);
        }
    }

    private <T> void consume(Actor<T> actor) {
        cpuBoundThreads.computeIfAbsent(actor.name(), ignored -> {
            var t = new SpinningThread<>(actor);
            t.start();
            return t;
        });
    }

    @Override
    public <T> void clear(Actor<T> actor) {
        Optional.ofNullable(cpuBoundThreads.get(actor.name()))
                .ifPresent(SpinningThread::halt);
        Mailbox<?> mb = mailboxes.remove(actor.name());
        if (mb != null) mb.clear();
    }

    private <T> Mailbox<T> mailboxFor(Actor<T> actor) {
        return (Mailbox<T>) mailboxes.computeIfAbsent(actor.name(), name ->
                new MpscMailbox<>(config.mailboxCapacity())
        );
    }

    private class SpinningThread<T> extends Thread {
        private final Actor<T> actor;
        private volatile boolean stopped = false;

        private SpinningThread(Actor<T> actor) {
            this.actor = actor;
            setName("bhpSpinningThread-" + actor.name());
            //never set daemon to false
            setDaemon(true);
        }

        @Override
        public void run() {
            try (AffinityLock lock = AffinityLock.acquireLock()) {
                var mailbox = mailboxFor(actor);
                T message = null;
                try {
                    while (!stopped && !Thread.currentThread().isInterrupted()) {
                        try {
                            message = mailbox.poll();
                            if (message != null)
                                actor.receive(message);
                        } catch (Exception e) {
                            actor.onError(message, e);
                        }
                        onSpinWait();
                    }
                } catch (BlockingMailbox.MailBoxInterrupted e) {
                    stopped = true;
                }
                cpuBoundThreads.remove(actor.name());
            } catch (Exception e) {
                cpuBoundThreads.remove(actor.name());
            }
        }

        void halt() {
            this.stopped = true;
        }
    }
}
