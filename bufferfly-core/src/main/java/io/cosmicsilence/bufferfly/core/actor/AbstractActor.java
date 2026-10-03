package io.cosmicsilence.bufferfly.core.actor;

import java.util.concurrent.atomic.AtomicBoolean;

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

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    protected ActorReference<T> self;

    public void setSelf(ActorReference<T> self) {
        this.self = self;
    }

    // -----------------------------------------------------------------------
    // Lifecycle — called by ActorManager / DispatchingActor, not by callers
    // -----------------------------------------------------------------------

    @Override
    public final void start() {
        if (started.compareAndSet(false, true)) {
            preStart();
        }
    }

    @Override
    public final boolean isStarted() {
        return started.get();
    }

    /**
     * Package-private stop hook invoked by {@link ActorManager.DispatchingActor}
     * when {@link ActorReference#stop()} is called on the reference.
     * Not part of the public {@link Actor} API — callers use {@link ActorReference#stop()}.
     */
    final void shutdown() {
        started.set(false);
        if (stopped.compareAndSet(false, true)) {
            postStop();
        }
    }

    // -----------------------------------------------------------------------
    // Extension points
    // -----------------------------------------------------------------------

    /**
     * Called once when the actor is first started.
     * Override to initialise resources, subscribe to topics, etc.
     */
    protected void preStart() {}

    /**
     * Called once when the actor is stopped via {@link ActorReference#stop()}.
     * Override to release resources, flush state, etc.
     */
    protected void postStop() {}
}
