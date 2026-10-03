package io.cosmicsilence.bufferfly.core.actor;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VTDispatcher implements Dispatcher {

    /** Default mailbox capacity used when no per-actor override is present. */
    private final int defaultMailboxCapacity;

    /**
     * Per-actor mailbox capacity overrides. Key = actor name, value = capacity.
     * Populated from {@link io.cosmicsilence.bufferfly.core.config.BufferflyConfig} by
     * {@link ActorManager}.
     */
    private final Map<String, Integer> mailboxCapacityOverrides;

    private final ExecutorService vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<Actor<?>, Future<?>> runningActors = new ConcurrentHashMap<>();
    private final Map<String, Mailbox<?>>  mailboxes     = new ConcurrentHashMap<>();

    /** Creates a dispatcher with the built-in default capacity (20 000). */
    public VTDispatcher() {
        this(20_000, Map.of());
    }

    /** Creates a dispatcher with a custom default capacity and no per-actor overrides. */
    public VTDispatcher(int defaultMailboxCapacity) {
        this(defaultMailboxCapacity, Map.of());
    }

    /**
     * Creates a dispatcher with a custom default capacity and per-actor mailbox
     * capacity overrides.
     *
     * @param defaultMailboxCapacity default capacity for actors without an override
     * @param mailboxCapacityOverrides map of actor-name → capacity; may be empty
     */
    public VTDispatcher(int defaultMailboxCapacity, Map<String, Integer> mailboxCapacityOverrides) {
        if (defaultMailboxCapacity <= 0) {
            throw new IllegalArgumentException(
                "defaultMailboxCapacity must be > 0, got: " + defaultMailboxCapacity);
        }
        this.defaultMailboxCapacity    = defaultMailboxCapacity;
        this.mailboxCapacityOverrides  = Map.copyOf(mailboxCapacityOverrides);
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
        runningActors.computeIfAbsent(actor, ignored -> vtExecutor.submit(() -> {
            var mailbox = mailboxFor(actor);
            boolean interrupted = false;
            T message;
            try {
                while ((message = mailbox.poll()) != null) {
                    try {
                        actor.receive(message);
                    } catch (Exception e) {
                        actor.onError(message, e);
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
        }));
    }

    @Override
    public <T> void clear(Actor<T> actor) {
        Optional.ofNullable(runningActors.get(actor)).ifPresent(f -> f.cancel(true));
        Mailbox<?> mb = mailboxes.remove(actor.name());
        if (mb != null) mb.clear();
    }

    private <T> Mailbox<T> mailboxFor(Actor<T> actor) {
        return (Mailbox<T>) mailboxes.computeIfAbsent(actor.name(), name -> {
            int capacity = mailboxCapacityOverrides.getOrDefault(name, defaultMailboxCapacity);
            return new BlockingMailbox<>(capacity);
        });
    }
}
