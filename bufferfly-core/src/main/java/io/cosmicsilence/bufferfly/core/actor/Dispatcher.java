package io.cosmicsilence.bufferfly.core.actor;

public interface Dispatcher {

   <T> void dispatch(T t, Actor<T> actor);

   <T> void clear(Actor<T> actor);
}
