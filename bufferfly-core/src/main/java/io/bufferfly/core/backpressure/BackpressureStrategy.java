package io.bufferfly.core.backpressure;

/**
 * Determines how a {@link BackpressureActorDispatcher} reacts to incoming
 * dispatches <em>while the system is under pressure</em>.
 *
 * <p>Under normal (unpressured) conditions both strategies behave identically:
 * dispatches are forwarded to the delegate dispatcher as usual. The difference
 * only manifests once {@link BackpressureCallback#onBackpressure()} has fired
 * and until {@link BackpressureCallback#onReleasedPressure()} resets it.
 */
public enum BackpressureStrategy {

    /**
     * Run the dispatch on the <em>caller's own thread</em> instead of handing
     * it off to the asynchronous (virtual-thread) dispatcher.
     *
     * <p>This deliberately "hijacks" the producer thread to execute the actor
     * work synchronously, throttling the upstream rate to match the speed at
     * which the downstream buffer can drain. It never loses messages; it simply
     * slows everything down — the classic {@code CallerRunsPolicy} applied to
     * the actor layer.
     */
    CALLER_RUNS,

    /**
     * Reject the dispatch outright by throwing {@link UnderPressureException}.
     *
     * <p>Fail-fast: the upstream producer is expected to stop producing (e.g.
     * pause a Kafka partition) and retry/replay once pressure is relieved.
     */
    THROW
}
