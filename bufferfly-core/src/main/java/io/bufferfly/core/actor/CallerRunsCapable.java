package io.bufferfly.core.actor;

/**
 * Capability implemented by dispatchers that can switch between asynchronous
 * dispatch and {@code CALLER_RUNS} dispatch at runtime.
 *
 * <p>A {@code CALLER_RUNS}-capable dispatcher, when switched on, drains an
 * actor's mailbox on the <em>calling thread</em> rather than handing off to its
 * internal executor — <strong>without</strong> reordering messages or breaking
 * the single-threaded-actor invariant, because both modes share the same
 * mailbox and the same single-consumer slot.
 *
 * <p>This lets a backpressure layer (see
 * {@code io.bufferfly.core.backpressure.BackpressureActorDispatcher}) throttle
 * producers by hijacking their threads instead of bypassing the mailbox with a
 * separate inline dispatcher.
 *
 * @see VTDispatcher
 */
public interface CallerRunsCapable {

    /**
     * Enables or disables {@code CALLER_RUNS} mode.
     *
     * @param callerRuns {@code true} to drain on the caller thread; {@code false}
     *                   to resume asynchronous dispatch
     */
    void setCallerRuns(boolean callerRuns);

    /** @return whether {@code CALLER_RUNS} mode is currently active. */
    boolean isCallerRuns();
}
