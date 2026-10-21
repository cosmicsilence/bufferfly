package io.bufferfly.core.actor;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class VTDispatcher implements ActorDispatcher, CallerRunsCapable {

    /**
     * Default mailbox capacity used when no per-actor override is present.
     */
    private final int defaultMailboxCapacity;

    /**
     * Per-actor mailbox capacity overrides. Key = actor name, value = capacity.
     * Populated from {@link io.bufferfly.core.config.BufferflyConfig} by
     * {@link ActorManager}.
     */
    private final Map<String, Integer> mailboxCapacityOverrides;

    //TODO make this threshold dynamic based on amount of actors and mailbox size with limits of min and max
    public static final int YIELD_THRESHOLD = 32;

    private final ExecutorService vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final PreStartBuffer preStartBuffer = new PreStartBuffer();

    /**
     * Per-actor single-consumer slot. The thread that successfully inserts an
     * {@link DrainOwner} here is the <em>only</em> thread allowed to drain that
     * actor's mailbox, guaranteeing the single-threaded-actor invariant across
     * both the asynchronous (virtual-thread) and the synchronous (caller-runs)
     * execution modes.
     */
    private final Map<Actor<?>, DrainOwner> runningActors = new ConcurrentHashMap<>();
    private final Map<String, Mailbox<?>> mailboxes = new ConcurrentHashMap<>();

    /**
     * When {@code true}, {@link #dispatch} drains the mailbox on the
     * <em>calling thread</em> instead of handing off to a virtual thread.
     * This is the {@code CALLER_RUNS} backpressure mode: the producer thread is
     * hijacked to perform the actor work, throttling the upstream rate.
     *
     * <p>Flipping this flag never breaks ordering or the single-consumer
     * invariant: both modes use the same mailbox and the same
     * {@link #runningActors} slot. If an async VT already owns the slot, a
     * caller-runs dispatch simply leaves its (already-enqueued) message for that
     * VT to drain; it never runs {@code receive} concurrently.
     */
    private volatile boolean callerRuns = false;

    /**
     * Creates a dispatcher with the built-in default capacity (20 000).
     */
    public VTDispatcher() {
        this(20_000, Map.of());
    }

    /**
     * Creates a dispatcher with a custom default capacity and no per-actor overrides.
     */
    public VTDispatcher(int defaultMailboxCapacity) {
        this(defaultMailboxCapacity, Map.of());
    }

    /**
     * Creates a dispatcher with a custom default capacity and per-actor mailbox
     * capacity overrides.
     *
     * @param defaultMailboxCapacity   default capacity for actors without an override
     * @param mailboxCapacityOverrides map of actor-name → capacity; may be empty
     */
    public VTDispatcher(int defaultMailboxCapacity, Map<String, Integer> mailboxCapacityOverrides) {
        if (defaultMailboxCapacity <= 0) {
            throw new IllegalArgumentException(
                    "defaultMailboxCapacity must be > 0, got: " + defaultMailboxCapacity);
        }
        this.defaultMailboxCapacity = defaultMailboxCapacity;
        this.mailboxCapacityOverrides = Map.copyOf(mailboxCapacityOverrides);
    }

    /**
     * Switches the dispatcher between asynchronous (virtual-thread) dispatch and
     * {@code CALLER_RUNS} dispatch. Safe to flip concurrently; in-flight drains
     * are unaffected and finish under the mode they started in.
     *
     * @param callerRuns {@code true} to drain on the caller thread; {@code false}
     *                   to hand off to virtual threads
     */
    @Override
    public void setCallerRuns(boolean callerRuns) {
        this.callerRuns = callerRuns;
    }

    /** @return whether the dispatcher is currently in {@code CALLER_RUNS} mode. */
    @Override
    public boolean isCallerRuns() {
        return callerRuns;
    }

    @Override
    public <T> void dispatch(T t, Actor<T> actor) {
        // Always enqueue first so ordering is preserved regardless of mode.
        final var accepted = mailboxFor(actor).offer(t);
        if (!accepted) {
            throw new IllegalStateException(
                    "Mailbox refused for the actor.name=%s".formatted(actor.name()));
        }
        consume(actor);
    }

    private <T> void consume(Actor<T> actor) {
        if (preStartBuffer.bufferIfPreStarting(actor, () -> consume(actor))) return;
        if (callerRuns) {
            consumeOnCaller(actor);
        } else {
            consumeOnVirtualThread(actor);
        }
    }

    /**
     * Async path: the slot winner submits a VT that drains the mailbox.
     */
    private <T> void consumeOnVirtualThread(Actor<T> actor) {
        runningActors.computeIfAbsent(actor, ignored -> {
            DrainOwner owner = new DrainOwner();
            owner.future = vtExecutor.submit(() -> drainLoop(actor));
            return owner;
        });
    }

    /**
     * Caller-runs path: if the slot is free, the calling thread itself drains
     * the mailbox inline. If a drain (VT or another caller) already owns the
     * slot, we return immediately — the enqueued message will be picked up by
     * that owner, preserving order and the single-consumer invariant.
     */
    private <T> void consumeOnCaller(Actor<T> actor) {
        DrainOwner self = new DrainOwner();
        DrainOwner existing = runningActors.putIfAbsent(actor, self);
        if (existing != null) {
            return; // someone else owns the drain loop; our message is queued for them
        }
        drainLoop(actor);
    }

    /**
     * The single drain loop shared by both modes. Only ever executed by the
     * thread that owns the {@link #runningActors} slot for {@code actor} — a
     * virtual thread in async mode, or the caller thread in {@code CALLER_RUNS}
     * mode. The double-check re-enters {@link #consume} which re-reads the
     * current mode, so a miss re-submits a VT or re-drains inline accordingly.
     */
    private <T> void drainLoop(Actor<T> actor) {
        var mailbox = mailboxFor(actor);
        boolean interrupted = false;
        T message;
        int executions = 0;
        try {
            while ((message = mailbox.poll()) != null) {
                try {
                    actor.receive(message);
                } catch (Exception e) {
                    actor.onError(message, e);
                }
                // protection against greedy actors, letting others be scheduled
                if (++executions >= YIELD_THRESHOLD) {
                    Thread.yield();
                    executions = 0;
                }
            }
        } catch (BlockingMailbox.MailBoxInterrupted e) {
            interrupted = true;
        }
        runningActors.remove(actor);
        // Double-check for messages that raced in after the loop exited.
        if (!interrupted && mailbox.size() > 0) {
            consume(actor);
        }
    }

    @Override
    public <T> void clear(Actor<T> actor) {
        Optional.ofNullable(runningActors.get(actor)).ifPresent(DrainOwner::cancel);
        Mailbox<?> mb = mailboxes.remove(actor.name());
        if (mb != null) mb.clear();
    }

    @Override
    public void stop() {
        runningActors.values().forEach(DrainOwner::cancel);
        runningActors.clear();
        mailboxes.clear();
        preStartBuffer.stop();
        vtExecutor.shutdown();
    }

    private <T> Mailbox<T> mailboxFor(Actor<T> actor) {
        return (Mailbox<T>) mailboxes.computeIfAbsent(actor.name(), name -> {
            int capacity = mailboxCapacityOverrides.getOrDefault(name, defaultMailboxCapacity);
            return new BlockingMailbox<>(capacity);
        });
    }

    /**
     * Ownership token for the per-actor drain slot.
     *
     * <p>For an asynchronous (VT) drain it holds the {@link Future} so
     * {@link #clear}/{@link #stop} can interrupt it. For a caller-runs drain
     * there is no future to cancel — the owning caller thread simply runs the
     * loop to completion; interrupting it would mean interrupting the producer.
     */
    private static final class DrainOwner {
        private volatile Future<?> future;

        void cancel() {
            Future<?> f = future;
            if (f != null) f.cancel(true);
        }
    }
}
