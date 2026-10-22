package io.bufferfly.core.actor;

/**
 * Internal contract for an actor implementation.
 *
 * <p>This interface is intentionally separate from {@link ActorReference}.
 * Callers never hold an {@code Actor} directly — they receive an
 * {@link ActorReference} from {@link ActorManager#start(Actor)} and interact
 * through that. This prevents calling {@code tell()} before the actor has
 * been registered and started by the manager.
 */
public interface Actor<T> {

    /** Called by the dispatcher to deliver one message. */
    void receive(T message);

    /**
     * Injects this actor's {@link ActorContext} — its {@code self} reference and
     * its one-shot {@link Scheduler}. Called exactly once by
     * {@link ActorManager#start(Actor)} before {@link #start()}.
     */
    void setContext(ActorContext<T> context);

    /** Unique name identifying this actor within an {@link ActorManager}. */
    String name();

    /** Called by the dispatcher when {@link #receive} throws an exception. */
    void onError(T message, Exception e);

    /**
     * Initialises the actor. Called exactly once by {@link ActorManager#start(Actor)}.
     * Idempotent — subsequent calls are no-ops.
     */
    void start();

    /**
     * Stops the actor. Called when ActorReference.stop() is called
     */
    void stop();

    AbstractActor.State currentState();

    enum State {
        NOT_STARTED,
        PRE_STARTING,
        STARTED,
        STOPPED;

        static boolean isStartingOrStartedState(State state) {
            return State.STARTED == state|| State.NOT_STARTED == state || State.PRE_STARTING == state;
        }
    }

}
