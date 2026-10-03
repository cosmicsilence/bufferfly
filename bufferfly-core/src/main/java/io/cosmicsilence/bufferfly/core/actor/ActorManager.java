package io.cosmicsilence.bufferfly.core.actor;

import io.cosmicsilence.bufferfly.core.config.BufferflyConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ActorManager {

    private final Map<String, ActorReference<?>> startedActors = new ConcurrentHashMap<>();

    /**
     * Per-actor dispatcher registry.
     * Key = actor name, value = the Dispatcher instance to use for that actor.
     * Falls back to {@link #defaultDispatcher} for actors not in this map.
     */
    private final Map<String, Dispatcher> configuredDispatchers;

    /**
     * The dispatcher used when no per-actor override is configured.
     */
    private final Dispatcher defaultDispatcher;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /**
     * Creates an ActorManager with default settings (VTDispatcher, capacity 20 000).
     */
    public ActorManager() {
        this.defaultDispatcher = new VTDispatcher();
        this.configuredDispatchers = new ConcurrentHashMap<>();
    }

    /**
     * Creates an ActorManager wired from a {@link BufferflyConfig}.
     *
     * <p>Wiring rules:
     * <ol>
     *   <li>A single shared default {@link Dispatcher} is instantiated from
     *       {@code config.actors.defaultDispatcher} (FQN, no-arg constructor).
     *       It receives the global default mailbox capacity plus the full map of
     *       per-actor capacity overrides so it can size each mailbox correctly.</li>
     *   <li>For each actor that specifies a <em>different</em> dispatcher FQN in its
     *       override, a dedicated {@link Dispatcher} instance is instantiated and
     *       registered in {@code configuredDispatchers}.</li>
     * </ol>
     */
    public ActorManager(BufferflyConfig config) {
        BufferflyConfig.ActorsConfig actors = config.getActors();

        Map<String, Integer> allCapacityOverrides = new HashMap<>();
        actors.getOverrides().forEach((actorName, override) -> {
            if (override.getMailboxCapacity() > 0) {
                allCapacityOverrides.put(actorName, override.getMailboxCapacity());
            }
        });

        this.defaultDispatcher = instantiateDispatcher(
                actors.getDefaultDispatcher(),
                actors.getDefaultMailboxCapacity(),
                allCapacityOverrides
        );

        Map<String, Dispatcher> perActor = new HashMap<>();
        actors.getOverrides().forEach((actorName, override) -> {
            String fqn = override.getDispatcher();
            if (fqn != null && !fqn.equals(actors.getDefaultDispatcher())) {
                int capacity = override.getMailboxCapacity() > 0
                        ? override.getMailboxCapacity()
                        : actors.getDefaultMailboxCapacity();
                perActor.put(actorName, instantiateDispatcher(fqn, capacity, Map.of()));
            }
        });
        this.configuredDispatchers = new ConcurrentHashMap<>(perActor);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Registers and starts {@code actor}, returning an {@link ActorReference}
     * through which callers can send messages and stop the actor.
     *
     * <p>If an actor with the same name is already registered, the existing
     * reference is returned and {@code actor} is ignored (idempotent).
     *
     * @param actor the actor implementation to register
     * @return a reference that provides {@code tell()} and {@code stop()}
     */
    public <T> ActorReference<T> start(Actor<T> actor) {
        final var registered = startedActors.computeIfAbsent(actor.name(), ignored -> {
            Dispatcher dispatcher = configuredDispatchers.getOrDefault(
                    actor.name(), defaultDispatcher);
            DispatchingActor<T> dispatchingActor = new DispatchingActor<>(actor, dispatcher);
            dispatchingActor.start();
            if (actor instanceof AbstractActor<T> abstractActor) {
                abstractActor.setSelf(dispatchingActor);
            }
            return dispatchingActor;
        });
        return (ActorReference<T>) registered;
    }

    // -----------------------------------------------------------------------
    // Dispatcher instantiation
    // -----------------------------------------------------------------------

    private static Dispatcher instantiateDispatcher(
            String fqn,
            int defaultCapacity,
            Map<String, Integer> capacityOverrides) {

        if (VTDispatcher.class.getName().equals(fqn)) {
            return new VTDispatcher(defaultCapacity, capacityOverrides);
        }
        try {
            Class<?> cls = Class.forName(fqn);
            if (!Dispatcher.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException(
                        "Class '%s' does not implement Dispatcher".formatted(fqn));
            }
            return (Dispatcher) cls.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Dispatcher class not found on classpath: " + fqn, e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate dispatcher '%s' via no-arg constructor".formatted(fqn), e);
        }
    }

    // -----------------------------------------------------------------------
    // Inner class: DispatchingActor
    // -----------------------------------------------------------------------

    /**
     * Package-private bridge between an {@link Actor} implementation and the
     * outside world. Implements only {@link ActorReference} — callers never
     * see the underlying {@link Actor}.
     *
     * <p>Routing: {@code tell()} → {@link Dispatcher#dispatch} → {@link Actor#receive}.
     * <p>Lifecycle: {@code start()} delegates to {@link Actor#start()};
     * {@code stop()} drains / clears the dispatcher and calls
     * {@link AbstractActor#shutdown()} if the delegate is an
     * {@link AbstractActor}.
     */
    final class DispatchingActor<T> implements ActorReference<T> {

        private final Actor<T> delegate;
        private final Dispatcher dispatcher;

        DispatchingActor(Actor<T> delegate, Dispatcher dispatcher) {
            this.delegate = delegate;
            this.dispatcher = dispatcher;
        }

        /**
         * Starts the underlying actor (idempotent).
         */
        void start() {
            delegate.start();
        }

        @Override
        public void tell(T message) {
            if (!delegate.isStarted()) {
                throw new IllegalStateException(
                        "Actor '%s' has not been started".formatted(delegate.name()));
            }
            dispatcher.dispatch(message, delegate);
        }

        @Override
        public void stop() {
            // Shut down the underlying actor lifecycle if it extends AbstractActor
            if (delegate instanceof AbstractActor<?> a) {
                a.shutdown();
            }
            dispatcher.clear(delegate);
            startedActors.remove(delegate.name());
        }
    }
}
