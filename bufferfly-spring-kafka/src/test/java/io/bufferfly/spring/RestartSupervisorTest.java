package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link RestartSupervisor} actor: it sequences the restart
 * steps correctly, backs off longer on successive rapid restarts, and forgets
 * released partitions — all driven through real actor messaging + the injected
 * {@link io.bufferfly.core.actor.Scheduler}, with a fake {@link RestartSupervisor.RestartActions}.
 */
@Timeout(60)
class RestartSupervisorTest {

    private static final TopicPartition TP0 = new TopicPartition("t", 0);
    private static final TopicPartition TP1 = new TopicPartition("t", 1);

    /** Records every action with a timestamp; "owns" all partitions unless released. */
    static final class RecordingActions implements RestartSupervisor.RestartActions {
        record Event(String action, TopicPartition tp, long atNanos) {
        }

        final List<Event> events = new CopyOnWriteArrayList<>();
        final Set<TopicPartition> owned = ConcurrentHashMap.newKeySet();

        RecordingActions(TopicPartition... owns) {
            owned.addAll(List.of(owns));
        }

        private void rec(String a, TopicPartition tp) {
            events.add(new Event(a, tp, System.nanoTime()));
        }

        @Override public void pause(TopicPartition tp) { rec("pause", tp); }
        @Override public void beginRewind(TopicPartition tp) { rec("beginRewind", tp); }
        @Override public void stopShard(TopicPartition tp) { rec("stop", tp); }
        @Override public void startShard(TopicPartition tp) { rec("start", tp); }
        @Override public void resume(TopicPartition tp) { rec("resume", tp); }
        @Override public boolean owns(TopicPartition tp) { return owned.contains(tp); }

        List<String> actionsFor(TopicPartition tp) {
            return events.stream().filter(e -> e.tp().equals(tp)).map(Event::action).toList();
        }

        long firstTimeOf(String action, TopicPartition tp, int occurrence) {
            return events.stream().filter(e -> e.action().equals(action) && e.tp().equals(tp))
                    .skip(occurrence).findFirst().orElseThrow().atNanos();
        }

        long count(String action, TopicPartition tp) {
            return events.stream().filter(e -> e.action().equals(action) && e.tp().equals(tp)).count();
        }
    }

    private ActorManager manager;

    @BeforeEach
    void setUp() {
        manager = new ActorManager();
    }

    @AfterEach
    void tearDown() {
        manager.shutdown();
    }

    private ActorReference<RestartSupervisor.SupervisorMsg> startSupervisor(RecordingActions actions) {
        return manager.start(new RestartSupervisor("test-supervisor", actions));
    }

    @Test
    void should_sequencePauseBeginRewindStop_thenStartResume_afterBackoff() {
        RecordingActions actions = new RecordingActions(TP0);
        ActorReference<RestartSupervisor.SupervisorMsg> sup = startSupervisor(actions);

        sup.tell(new RestartSupervisor.RestartRequested(TP0));

        await().untilAsserted(() -> assertEquals(
                List.of("pause", "beginRewind", "stop", "start", "resume"),
                actions.actionsFor(TP0),
                "restart must run pause -> beginRewind -> stop -> (backoff) -> start -> resume"));

        // start must come AFTER stop (the backoff gap sits between them)
        long stopAt = actions.firstTimeOf("stop", TP0, 0);
        long startAt = actions.firstTimeOf("start", TP0, 0);
        assertTrue(startAt > stopAt, "start happens after stop (post-backoff)");
    }

    @Test
    void should_backOffLonger_onSuccessiveRapidRestarts() {
        RecordingActions actions = new RecordingActions(TP0);
        ActorReference<RestartSupervisor.SupervisorMsg> sup = startSupervisor(actions);

        // First restart -> base backoff (~100ms)
        sup.tell(new RestartSupervisor.RestartRequested(TP0));
        await().untilAsserted(() -> assertEquals(1, actions.count("start", TP0)));
        long gap1 = actions.firstTimeOf("start", TP0, 0) - actions.firstTimeOf("stop", TP0, 0);

        // Second restart immediately -> next backoff (~200ms), measurably longer
        sup.tell(new RestartSupervisor.RestartRequested(TP0));
        await().untilAsserted(() -> assertEquals(2, actions.count("start", TP0)));
        long gap2 = actions.firstTimeOf("start", TP0, 1) - actions.firstTimeOf("stop", TP0, 1);

        assertTrue(gap2 > gap1,
                "second consecutive restart must back off longer (gap1=" + gap1 / 1_000_000
                        + "ms, gap2=" + gap2 / 1_000_000 + "ms)");
    }

    @Test
    void should_keepPartitionsIndependent() {
        RecordingActions actions = new RecordingActions(TP0, TP1);
        ActorReference<RestartSupervisor.SupervisorMsg> sup = startSupervisor(actions);

        // ramp TP0 twice, then restart TP1 once
        sup.tell(new RestartSupervisor.RestartRequested(TP0));
        await().untilAsserted(() -> assertEquals(1, actions.count("start", TP0)));
        sup.tell(new RestartSupervisor.RestartRequested(TP0));
        await().untilAsserted(() -> assertEquals(2, actions.count("start", TP0)));

        long tp0Gap2 = actions.firstTimeOf("start", TP0, 1) - actions.firstTimeOf("stop", TP0, 1);

        sup.tell(new RestartSupervisor.RestartRequested(TP1));
        await().untilAsserted(() -> assertEquals(1, actions.count("start", TP1)));
        long tp1Gap1 = actions.firstTimeOf("start", TP1, 0) - actions.firstTimeOf("stop", TP1, 0);

        // TP1's FIRST restart (fresh ramp) is shorter than TP0's SECOND restart
        assertTrue(tp1Gap1 < tp0Gap2,
                "TP1 starts a fresh backoff ramp independent of TP0 (tp1=" + tp1Gap1 / 1_000_000
                        + "ms, tp0#2=" + tp0Gap2 / 1_000_000 + "ms)");
    }

    @Test
    void should_notStart_whenPartitionReleasedBeforeBackoffElapses() {
        RecordingActions actions = new RecordingActions(TP0);
        ActorReference<RestartSupervisor.SupervisorMsg> sup = startSupervisor(actions);

        sup.tell(new RestartSupervisor.RestartRequested(TP0));
        // Release the partition immediately (revoke during the backoff window).
        sup.tell(new RestartSupervisor.PartitionReleased(TP0));

        // The pause/stop happened, but start/resume must NOT (the backoff timer
        // finds the partition released and aborts).
        await().untilAsserted(() -> assertTrue(actions.count("stop", TP0) >= 1, "stop happened"));
        try {
            Thread.sleep(400); // well past the ~100ms base backoff
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(0, actions.count("start", TP0),
                "a partition released during the backoff window must NOT be restarted");
    }
}
