package io.cosmicsilence.bufferfly.core.actor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingMailbox<T> implements Mailbox<T> {
    private final BlockingQueue<T> mailbox;
    private static final long WAIT_MS = 10_000L;

    public BlockingMailbox(int capacity) {
        this.mailbox = new LinkedBlockingQueue<>(capacity);
    }


    @Override
    public boolean offer(T t) {
        return interrupted(() -> mailbox.offer(t, WAIT_MS, TimeUnit.MILLISECONDS));
    }

    @Override
    public T poll() {
        return interrupted(() -> mailbox.poll(WAIT_MS, TimeUnit.MILLISECONDS));
    }

    @Override
    public void clear() {
        mailbox.clear();
    }

    @Override
    public int size() {
        return mailbox.size();
    }

    private interface Interrupted<TP> {
        TP run() throws InterruptedException;
    }

    private <TP> TP interrupted(Interrupted<TP> command) {
        try {
            return command.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MailBoxInterrupted();
        }
    }
    static class MailBoxInterrupted extends RuntimeException {
    }
}
