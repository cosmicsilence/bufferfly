package io.bufferfly.core.actor;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Convenience base class for {@link Actor} implementations.
 *
 * <p>Manages the start/stop lifecycle flags so subclasses only need to
 * implement {@link #receive}, {@link #name}, {@link #onError},
 * {@link #preStart}, and {@link #postStop}.
 *
 * <p>{@code tell()} and {@code stop()} are intentionally absent here.
 * Callers interact through the {@link ActorReference} returned by
 * {@link ActorManager#start(Actor)} — never directly through the actor
 * instance. This enforces the contract that an actor can only be reached
 * after it has been registered and started.
 */
public abstract class AbstractActor<T> implements Actor<T> {

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    /**
     * The capabilities injected at start: this actor's {@code self} reference and
     * its one-shot {@link Scheduler}. Reach them via {@code context.self()},
     * {@code context.scheduler()} or {@code context.schedule(...)}. {@code null}
     * until {@link #setContext} is called by the {@link ActorManager}.
     */
    protected ActorContext<T> context;

    @Override
    public void setContext(ActorContext<T> context) {
        this.context = context;
    }

    @Override
    public final void start() {
        if (state.compareAndSet(State.NOT_STARTED, State.PRE_STARTING)) {
            preStart();
            state.set(State.STARTED);
        }
    }

    @Override
    public void stop() {
        var current = state.get();
        if (State.isStartingOrStartedState(current)) {
            if (state.compareAndSet(current, State.STOPPED)) {
                postStop();
            }
        }

    }

    /**
     * Called once when the actor is first started.
     * Override to initialise resources, subscribe to topics, etc.
     */
    protected void preStart() {
    }

    /**
     * Called once when the actor is stopped via {@link ActorReference#stop()}.
     * Override to release resources, flush state, etc.
     */
    protected void postStop() {
    }

    public State currentState() {
        return state.get();
    }

}

