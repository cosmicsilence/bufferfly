package io.cosmicsilence.bufferfly.core.actor;

public interface ActorReference<T> {
    void tell(T t);

    void stop();
}
