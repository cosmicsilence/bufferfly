package io.bufferfly.cpu.bound;

import io.bufferfly.core.actor.Mailbox;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscUnboundedXaddArrayQueue;

//Not public - Maybe should be private do @SpinningDispather
class MpscMailbox<T> implements Mailbox<T> {

    static final int DEFAULT_CAPACITY = 100_000;

    private final MpscUnboundedXaddArrayQueue<T> queue;

    MpscMailbox() {
        this(DEFAULT_CAPACITY);
    }

    MpscMailbox(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be >= 1, got " + capacity);
        this.queue = new MpscUnboundedXaddArrayQueue<>(capacity);
    }

    @Override
    public boolean offer(T t) {
        return queue.offer(t);
    }

    @Override
    public T poll() {
        return queue.poll();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public int size() {
        return queue.size();
    }
}
