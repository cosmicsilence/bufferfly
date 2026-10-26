package io.bufferfly.core.actor;

import io.bufferfly.core.actor.Actor.State;
import io.bufferfly.core.config.BufferflyConfig;
import io.bufferfly.core.metrics.BufferflyMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ActorManager {

    private final Map<String, DispatchingActor<?>> startedActors = new ConcurrentHashMap<>();

    private final ExecutorService callStartExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Shared scheduler backing every actor's {@link Scheduler} capability. A
     * small daemon pool; each scheduled task does nothing but a cheap
     * {@code self.tell} hand-off, so a handful of threads serve all actors.
     */
    private final java.util.concurrent.ScheduledExecutorService scheduledExecutor =
            java.util.concurrent.Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "bufferfly-actor-scheduler");
                t.setDaemon(true);
                return t;
            });
    /**
     * Per-actor dispatcher registry.
     * Key = actor name, value = the Dispatcher instance to use for that actor.
     * Falls back to {@link #defaultDispatcher} for actors not in this map.
     */
    private final Map<String, ActorDispatcher> configuredDispatchers;

    /**
     * The dispatcher used when no per-actor override is configured.
     */
    private final ActorDispatcher defaultDispatcher;

    /**
     * Creates an ActorManager with default settings: a single {@link VTDispatcher}
     * (virtual-thread, default mailbox capacity 100 000) shared by every actor,
     * and no per-actor overrides.
     */
    public ActorManager() {
        this(new VTDispatcher(), Map.of());
    }

    /**
     * Creates an ActorManager wired from a {@link BufferflyConfig}.
     *
     * <p>The config is resolved into a shared default dispatcher plus any
     * per-actor dispatcher overrides by {@link DispatcherWiring#from}; see that
     * method for the wiring rules. Invalid dispatcher class names surface as
     * {@link IllegalArgumentException} here, at construction time.
     */
    public ActorManager(BufferflyConfig config) {
        this(DispatcherWiring.from(config, BufferflyMetrics.NOOP));
    }

    /**
     * Creates an ActorManager wired from a {@link BufferflyConfig} with an
     * observability sink. The {@code metrics} collector is handed to the default
     * {@link VTDispatcher} so mailbox enqueue/refuse counters and the mailbox
     * depth gauge are reported. Per-actor dispatchers configured via a different
     * FQN get the same collector when they are a {@link VTDispatcher}.
     *
     * @param config  the configuration to resolve dispatchers from
     * @param metrics the collector to report mailbox events to; use
     *                {@link BufferflyMetrics#NOOP} to disable
     */
    public ActorManager(BufferflyConfig config, BufferflyMetrics metrics) {
        this(DispatcherWiring.from(config, metrics == null ? BufferflyMetrics.NOOP : metrics));
    }

    private ActorManager(DispatcherWiring wiring) {
        this(wiring.defaultDispatcher(), wiring.perActorDispatchers());
    }

    /**
     * Canonical constructor: every other constructor funnels here with a fully
     * resolved default dispatcher and per-actor dispatcher map.
     *
     * @param defaultDispatcher   dispatcher used for actors without an override
     * @param perActorDispatchers per-actor dispatcher overrides, keyed by actor name
     */
    private ActorManager(ActorDispatcher defaultDispatcher, Map<String, ActorDispatcher> perActorDispatchers) {
        this.defaultDispatcher = defaultDispatcher;
        this.configuredDispatchers = new ConcurrentHashMap<>(perActorDispatchers);
    }

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
            ActorDispatcher dispatcher = configuredDispatchers.getOrDefault(
                    actor.name(), defaultDispatcher);
            DispatchingActor<T> dispatchingActor = new DispatchingActor<>(actor, dispatcher);
            callStartExecutor.execute(dispatchingActor::start);
            return dispatchingActor;
        });
        return (ActorReference<T>) registered;
    }

    /**
     * Releases the manager's shared executors. The scheduler pool is daemon so
     * it never blocks JVM exit; call this for a clean shutdown (e.g. in tests or
     * a container stop) to stop the scheduler threads promptly.
     */
    public void shutdown() {
        scheduledExecutor.shutdownNow();
        callStartExecutor.shutdownNow();
    }

    private static ActorDispatcher instantiateDispatcher(
            String fqn,
            int defaultCapacity,
            Map<String, Integer> capacityOverrides,
            BufferflyMetrics metrics) {

        if (VTDispatcher.class.getName().equals(fqn)) {
            return new VTDispatcher(defaultCapacity, capacityOverrides, metrics);
        }
        try {
            Class<?> cls = Class.forName(fqn);
            if (!ActorDispatcher.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException(
                        "Class '%s' does not implement Dispatcher".formatted(fqn));
            }
            return (ActorDispatcher) cls.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Dispatcher class not found on classpath: " + fqn, e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate dispatcher '%s' via no-arg constructor".formatted(fqn), e);
        }
    }

    /**
     * The dispatchers resolved from a {@link BufferflyConfig}: one shared default
     * plus any per-actor overrides. Isolating this derivation keeps
     * {@link ActorManager} focused on its real job — the actor registry and
     * lifecycle — while the config-to-dispatcher mapping lives in one cohesive,
     * independently testable place.
     *
     * @param defaultDispatcher   dispatcher for actors without a dispatcher override
     * @param perActorDispatchers dedicated dispatchers keyed by actor name
     */
    private record DispatcherWiring(ActorDispatcher defaultDispatcher,
                                    Map<String, ActorDispatcher> perActorDispatchers) {

        /**
         * Resolves a {@link BufferflyConfig} into its dispatchers.
         *
         * <ol>
         *   <li>A single shared default dispatcher is instantiated from
         *       {@code actors.defaultDispatcher} (FQN, no-arg constructor), given
         *       the global default mailbox capacity plus every per-actor capacity
         *       override so it can size each mailbox correctly.</li>
         *   <li>Each actor whose override names a <em>different</em> dispatcher FQN
         *       gets its own dedicated dispatcher, sized by its own capacity
         *       override (falling back to the global default).</li>
         * </ol>
         */
        static DispatcherWiring from(BufferflyConfig config, BufferflyMetrics metrics) {
            BufferflyConfig.ActorsConfig actors = config.getActors();
            String defaultFqn = actors.getDefaultDispatcher();
            int defaultCapacity = actors.getDefaultMailboxCapacity();
            Map<String, BufferflyConfig.ActorOverrideConfig> overrides = actors.getOverrides();

            ActorDispatcher defaultDispatcher =
                    instantiateDispatcher(defaultFqn, defaultCapacity, capacityOverrides(overrides), metrics);

            Map<String, ActorDispatcher> perActor = new HashMap<>();
            overrides.forEach((actorName, override) -> {
                String fqn = override.getDispatcher();
                if (fqn != null && !fqn.equals(defaultFqn)) {
                    perActor.put(actorName,
                            instantiateDispatcher(fqn, resolveCapacity(override, defaultCapacity), Map.of(), metrics));
                }
            });

            return new DispatcherWiring(defaultDispatcher, perActor);
        }

        /** The subset of overrides that specify a positive mailbox capacity. */
        private static Map<String, Integer> capacityOverrides(
                Map<String, BufferflyConfig.ActorOverrideConfig> overrides) {
            Map<String, Integer> capacities = new HashMap<>();
            overrides.forEach((actorName, override) -> {
                if (override.getMailboxCapacity() > 0) {
                    capacities.put(actorName, override.getMailboxCapacity());
                }
            });
            return capacities;
        }

        /** The override's capacity if set (&gt; 0), otherwise the global default. */
        private static int resolveCapacity(BufferflyConfig.ActorOverrideConfig override, int defaultCapacity) {
            return override.getMailboxCapacity() > 0 ? override.getMailboxCapacity() : defaultCapacity;
        }
    }

    /**
     * Package-private bridge between an {@link Actor} implementation and the
     * outside world. Implements only {@link ActorReference} — callers never
     * see the underlying {@link Actor}.
     *
     * <p>Routing: {@code tell()} → {@link ActorDispatcher#dispatch} → {@link Actor#receive}.
     * <p>Lifecycle: {@code start()} delegates to {@link Actor#start()};
     * {@code stop()} drains / clears the dispatcher and calls
     * {@link AbstractActor#stop()} if the delegate is an
     * {@link AbstractActor}.
     */
    private final class DispatchingActor<T> implements ActorReference<T> {

        private final Actor<T> delegate;
        private final ActorDispatcher dispatcher;

        DispatchingActor(Actor<T> delegate, ActorDispatcher dispatcher) {
            this.delegate = delegate;
            this.dispatcher = dispatcher;
        }

        void start() {
            // Inject the actor's context: its own reference (this) plus a
            // one-shot self-scheduler backed by the shared scheduled executor.
            delegate.setContext(new ActorContext<>(this, new SelfTellScheduler<>(this, scheduledExecutor)));
            delegate.start();
        }

        @Override
        public void tell(T message) {
            final var state = delegate.currentState();
            if (!State.isStartingOrStartedState(state)) {
                throw new ActorNotStartedException(
                        "Actor='%s' is not started, state=%s".formatted(delegate.name(), state));
            }
            dispatcher.dispatch(message, delegate);
        }

        @Override
        public void stop() {
            delegate.stop();
            dispatcher.clear(delegate);
            startedActors.remove(delegate.name());
        }
    }
}
