package io.bufferfly.core.actor;

public interface ActorReference<T> {

    void tell(T t);

//    <R> CompletableFuture<R> ask(T t);

    void stop();
}
