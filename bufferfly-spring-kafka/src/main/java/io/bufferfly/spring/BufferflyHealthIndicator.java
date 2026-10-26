package io.bufferfly.spring;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.List;
import java.util.Objects;

/**
 * A deliberately <b>minimal, stateless</b> Spring Boot Actuator health check for
 * BufferFly's sharded Kafka consumers.
 *
 * <h2>Design principle: only report DOWN for what a pod restart can fix</h2>
 * A health check drives orchestration decisions — above all, a Kubernetes
 * liveness/readiness probe going DOWN typically <b>restarts the pod</b>. So the
 * only honest thing to report DOWN for is a condition a restart actually
 * resolves. For a BufferFly consumer that is exactly one thing: <b>the Kafka
 * listener container is not running</b> (its consumer thread died, or it failed
 * to start — e.g. a bug in wiring/{@code preStart} that left the container down).
 *
 * <p>Everything else that could look "unhealthy" is deliberately <b>not</b>
 * checked here, because a restart would not help (and often hurts):
 * <ul>
 *   <li><b>A paused partition (backpressure)</b> is the framework working
 *       <em>correctly</em> — the shock absorber telling Kafka to wait while the
 *       database catches up. Restarting the pod drops the in-memory buffer and
 *       re-saturates the database on the replacement — a kill-loop that amplifies
 *       the incident. Never a health failure.</li>
 *   <li><b>An actor crash-looping on a poison pill</b> is handled in-process by
 *       the restart supervisor (backoff + dead-lettering). A pod restart just
 *       moves the same bad-data loop to a fresh pod; it does not fix the data.</li>
 *   <li><b>Rebalance churn</b> (partitions momentarily assigned-but-starting, or
 *       just revoked) is transient and self-healing. Reacting to it would make
 *       the probe flap on every deploy/scale event. Because this indicator never
 *       inspects partition-level state, it is immune to rebalance staleness.</li>
 * </ul>
 * Those conditions are surfaced through <b>metrics</b> (see
 * {@link io.bufferfly.core.metrics.BufferflyMetrics}:
 * {@code partition.paused}, {@code poison_pills}, {@code actor.restarted}) and
 * belong on dashboards/alerts — "is the system behaving well?" — not on a
 * restart decision — "should this pod exist?".
 *
 * <h2>Verdict</h2>
 * <ul>
 *   <li><b>UP</b> — every discovered {@code @ShardedActor} container is running.</li>
 *   <li><b>DOWN</b> — at least one container is not running. Details list each
 *       container's {@code running} state so an operator sees which one.</li>
 *   <li><b>UP (no containers)</b> — nothing to be unhealthy about.</li>
 * </ul>
 *
 * <p>Stateless: it reads each container's {@code isRunning()} live on every
 * probe. There is no cached partition state to go stale on a rebalance.
 *
 * <p>Registered automatically only when Spring Boot Actuator's health API is on
 * the classpath — see {@code BufferflySpringAutoConfiguration}.
 */
public final class BufferflyHealthIndicator implements HealthIndicator {

    private final ShardedActorBeanPostProcessor postProcessor;

    public BufferflyHealthIndicator(ShardedActorBeanPostProcessor postProcessor) {
        this.postProcessor = Objects.requireNonNull(postProcessor, "postProcessor");
    }

    @Override
    public Health health() {
        List<ShardedActorContainer<Object, Object>> containers = postProcessor.containers();

        Health.Builder builder = Health.up();
        boolean allRunning = true;

        for (ShardedActorContainer<Object, Object> c : containers) {
            boolean running = c.isRunning();
            builder.withDetail(c.name(), running ? "running" : "not running");
            if (!running) {
                allRunning = false;
            }
        }
        builder.withDetail("containers", containers.size());

        return allRunning ? builder.build() : builder.down().build();
    }
}
