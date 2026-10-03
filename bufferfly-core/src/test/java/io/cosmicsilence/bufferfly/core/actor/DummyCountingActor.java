package io.cosmicsilence.bufferfly.core.actor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal Actor implementation used exclusively in tests.
 *
 * <p>Exposes simple counters and a received-message list that Awaitility
 * can poll directly — no latches or custom waiting code needed in tests.
 */
public class DummyCountingActor extends AbstractActor<String> {

    private final String actorName;
    private final AtomicInteger receiveCount = new AtomicInteger(0);
    private final List<String> received = Collections.synchronizedList(new ArrayList<>());
    private final List<String> errorMessages = Collections.synchronizedList(new ArrayList<>());

    /** Optional simulated per-message processing delay (millis). */
    private volatile long processingDelayMs = 0;

    /** When non-null the actor throws on every message whose text equals this value. */
    private volatile String failOnMessage = null;

    public DummyCountingActor(String name) {
        this.actorName = name;
    }

    // -----------------------------------------------------------------------
    // Test configuration helpers (call before sending messages)
    // -----------------------------------------------------------------------

    public void setProcessingDelayMs(long ms) {
        this.processingDelayMs = ms;
    }

    public void setFailOnMessage(String text) {
        this.failOnMessage = text;
    }

    // -----------------------------------------------------------------------
    // Actor contract
    // -----------------------------------------------------------------------

    @Override
    public void receive(String message) {
        if (message.equals(failOnMessage)) {
            throw new IllegalArgumentException("Simulated failure for: " + message);
        }
        if (processingDelayMs > 0) {
            try {
                Thread.sleep(processingDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        received.add(message);
        receiveCount.incrementAndGet();
    }

    @Override
    public String name() {
        return actorName;
    }

    @Override
    protected void preStart() { }

    @Override
    protected void postStop() { }

    @Override
    public void onError(String message, Exception e) {
        errorMessages.add(message);
    }

    // -----------------------------------------------------------------------
    // Assertion helpers polled by Awaitility
    // -----------------------------------------------------------------------

    public int getReceiveCount() {
        return receiveCount.get();
    }

    public List<String> getReceived() {
        return Collections.unmodifiableList(received);
    }

    public List<String> getErrorMessages() {
        return Collections.unmodifiableList(errorMessages);
    }

    /** Reset all state — useful when reusing the same actor across test phases. */
    public void reset() {
        receiveCount.set(0);
        received.clear();
        errorMessages.clear();
    }
}
