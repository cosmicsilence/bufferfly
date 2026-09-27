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

    /** Unique name identifying this actor within an {@link ActorManager}. */
    String name();

    /** Called by the dispatcher when {@link #receive} throws an exception. */
    void onError(T message, Exception e);

    /**
     * Initialises the actor. Called exactly once by {@link ActorManager#start(Actor)}.
     * Idempotent — subsequent calls are no-ops.
     */
    void start();

    /** Returns {@code true} once {@link #start()} has been called successfully. */
    boolean isStarted();
}
