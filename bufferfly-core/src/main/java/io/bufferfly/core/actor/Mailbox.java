package io.bufferfly.core.actor;

public interface Mailbox<T> {

    boolean offer(T t);

    T poll();

    void clear();

    int size();

}
