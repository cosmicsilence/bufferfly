package io.bufferfly.spring;

import io.bufferfly.core.actor.AbstractActor;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * A single internal actor that supervises actor restarts for <b>all</b>
 * partitions of one {@link ShardedActorContainer}, owning the restart + backoff
 * state and the restart sequencing.
 *
 * <h2>Why an actor</h2>
 * Restart supervision is a classic single-writer problem: per-partition restart
 * counters and timings are mutated as failures and timers interleave. Rather
 * than guard them with {@code synchronized}/locks and a background
 * {@code ScheduledExecutorService}, this supervisor holds them as <b>plain
 * fields</b> and mutates them only inside {@link #receive} — on its own single
 * actor thread. Concurrency (the {@code onUnrecoverable} hook firing on a
 * persistence thread, the backoff timer firing on the scheduler thread) is
 * funnelled into ordered messages; there are no locks anywhere.
 *
 * <h2>Backoff timing via the actor {@link io.bufferfly.core.actor.Scheduler}</h2>
 * The wait between "stop the failed actor" and "start a fresh one" is armed with
 * {@code context.schedule(new PerformRestart(tp), backoff)} — a one-shot
 * self-message. When it fires, the supervisor is back on its own thread to run
 * the start/resume step. No sleeping, no shared executor in the container.
 *
 * <h2>Restart sequence (per partition)</h2>
 * <ol>
 *   <li>{@link RestartRequested} → pause the partition, arm the rewind gate,
 *       stop the current actor, compute the next backoff, schedule a
 *       {@link PerformRestart} to self after that delay;</li>
 *   <li>{@link PerformRestart} → start a fresh actor (its {@code preStart}
 *       rewinds to the DB offset, clearing the gate) and resume the partition.</li>
 * </ol>
 *
 * <h2>Backoff policy</h2>
 * Exponential {@code base × 2^n} capped at {@link #CAP_MS}; on reaching the cap
 * the ramp resets to the base (a bounded sawtooth that retries forever without
 * busy-looping). A partition healthy longer than the cap also resets, so an
 * unrelated later failure starts from zero backoff. State is dropped on
 * {@link PartitionReleased} (genuine revoke) so a reassigned partition does not
 * inherit a stale ramp.
 */
final class RestartSupervisor extends AbstractActor<RestartSupervisor.SupervisorMsg> {

    private static final Logger log = LoggerFactory.getLogger(RestartSupervisor.class);

    static final long BASE_MS = 100L;
    static final long CAP_MS = 30_000L;

    /** Messages the supervisor reacts to. */
    sealed interface SupervisorMsg permits RestartRequested, PerformRestart, PartitionReleased {
    }

    /** "This partition's actor hit an unrecoverable failure; restart it." */
    record RestartRequested(TopicPartition tp) implements SupervisorMsg {
    }

    /** Self-scheduled after the backoff: "now actually start the fresh actor." */
    record PerformRestart(TopicPartition tp, long generation) implements SupervisorMsg {
    }

    /** "This partition was revoked; forget its restart state." */
    record PartitionReleased(TopicPartition tp) implements SupervisorMsg {
    }

    /** The concrete restart steps, implemented by the container. */
    interface RestartActions {
        /** Pause fetching for the partition (idempotent). */
        void pause(TopicPartition tp);

        /** Arm the rewind gate so stale records are discarded until the new actor rewinds. */
        void beginRewind(TopicPartition tp);

        /** Stop the current actor for the partition. */
        void stopShard(TopicPartition tp);

        /** Start a fresh actor for the partition (its preStart rewinds + clears the gate). */
        void startShard(TopicPartition tp);

        /** Resume fetching for the partition. */
        void resume(TopicPartition tp);

        /** @return whether this container still owns the partition (not revoked). */
        boolean owns(TopicPartition tp);
    }

    /** Per-partition restart bookkeeping — mutated only on the actor thread. */
    private static final class RestartState {
        int consecutiveRestarts = 0;
        long lastRestartAtMs = 0L;
        /** Bumped on each RestartRequested; a PerformRestart with a stale generation is ignored. */
        long generation = 0L;
    }

    private final String name;
    private final RestartActions actions;

    /** Plain HashMap: only ever touched on the single actor thread. No locks. */
    private final Map<TopicPartition, RestartState> states = new HashMap<>();

    RestartSupervisor(String name, RestartActions actions) {
        this.name = name;
        this.actions = actions;
    }

    @Override
    public void receive(SupervisorMsg message) {
        switch (message) {
            case RestartRequested r -> onRestartRequested(r.tp());
            case PerformRestart p -> onPerformRestart(p.tp(), p.generation());
            case PartitionReleased rel -> states.remove(rel.tp());
        }
    }

    private void onRestartRequested(TopicPartition tp) {
        if (!actions.owns(tp)) {
            return; // partition no longer owned here (e.g. revoked meanwhile)
        }
        RestartState st = states.computeIfAbsent(tp, p -> new RestartState());
        long backoffMs = nextBackoff(st);
        long generation = ++st.generation;

        log.warn("Restarting actor for {} (unrecoverable failure) with backoff {}ms -> will rewind", tp, backoffMs);

        // 1) pause inflow, 2) arm rewind gate + stop the failed actor — now.
        actions.pause(tp);
        actions.beginRewind(tp);
        actions.stopShard(tp);

        // 3) wait the backoff via a one-shot self-message (no sleeping, no locks).
        context.schedule(new PerformRestart(tp, generation), Duration.ofMillis(backoffMs));
    }

    private void onPerformRestart(TopicPartition tp, long generation) {
        RestartState st = states.get(tp);
        if (st == null || generation != st.generation) {
            // Superseded by a newer restart request, or the partition was released
            // during the backoff window. Ignore this stale timer.
            return;
        }
        if (!actions.owns(tp)) {
            states.remove(tp);
            return;
        }
        // 4) start the fresh actor (preStart rewinds + clears the gate) and resume.
        actions.startShard(tp);
        actions.resume(tp);
    }

    /**
     * Exponential-with-reset-at-cap backoff. Mutates {@code st} in place; only
     * ever called on the actor thread, so no synchronization is needed.
     */
    private long nextBackoff(RestartState st) {
        long now = System.currentTimeMillis();
        if (st.lastRestartAtMs != 0L && (now - st.lastRestartAtMs) > CAP_MS) {
            st.consecutiveRestarts = 0; // healed: start fresh
        }
        long delay = BASE_MS << Math.min(st.consecutiveRestarts, 30);
        if (delay >= CAP_MS) {
            delay = CAP_MS;
            st.consecutiveRestarts = 0; // reached cap -> reset the ramp
        } else {
            st.consecutiveRestarts++;
        }
        st.lastRestartAtMs = now;
        return delay;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onError(SupervisorMsg message, Exception e) {
        log.error("RestartSupervisor error handling {}", message, e);
    }
}
