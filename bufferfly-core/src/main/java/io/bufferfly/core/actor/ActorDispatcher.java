package io.bufferfly.core.actor;

public interface ActorDispatcher {

   <T> void dispatch(T t, Actor<T> actor);

   <T> void clear(Actor<T> actor);

   void stop();
}