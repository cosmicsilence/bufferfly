package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.config.BufferflyConfig;
import io.bufferfly.core.metrics.BufferflyMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Auto-configuration entry point for the bufferfly-spring-kafka module.
 *
 * <p>Registers:
 * <ul>
 *   <li>a default {@link BufferflyMetrics} ({@link BufferflyMetrics#NOOP}) unless
 *       the application provides one (e.g. the OpenTelemetry adapter);</li>
 *   <li>a default {@link ActorManager} (unless the application provides one),
 *       wired with the {@link BufferflyMetrics} so mailbox depth/enqueue/refuse
 *       are observed; and</li>
 *   <li>the {@link ShardedActorBeanPostProcessor} that discovers
 *       {@link ShardedActor}-annotated factory beans and wires each to a
 *       {@link ShardedActorContainer} (one actor per partition), threading the
 *       {@link BufferflyMetrics} through so partition pause/resume, restarts,
 *       assign/revoke and record-rejected are observed.</li>
 * </ul>
 *
 * <p>The {@link ConsumerFactory} is expected to be provided by Spring Boot's
 * Kafka auto-configuration (via {@code spring.kafka.*} properties).
 *
 * <p>Observability is strictly opt-in: with no {@link BufferflyMetrics} bean the
 * default is {@link BufferflyMetrics#NOOP}, so nothing is recorded. Add the
 * {@code bufferfly-observability-otel} adapter (or any implementation) as a bean
 * to turn it on.
 */
@AutoConfiguration
public class BufferflySpringAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public BufferflyMetrics bufferflyMetrics() {
        return BufferflyMetrics.NOOP;
    }

    @Bean
    @ConditionalOnMissingBean
    public ActorManager bufferflyActorManager(BufferflyMetrics bufferflyMetrics) {
        // Wire the collector into the default VTDispatcher so mailbox metrics
        // (enqueue/refuse counters + depth gauge) are reported.
        return new ActorManager(BufferflyConfig.builder().build(), bufferflyMetrics);
    }

    /**
     * Registered as an infrastructure-role bean. {@code BeanPostProcessor} beans
     * should be as free of dependencies as possible; we take the
     * {@link ConsumerFactory}, {@link ActorManager} and {@link BufferflyMetrics}
     * as constructor args which Spring resolves eagerly.
     */
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public static ShardedActorBeanPostProcessor shardedActorBeanPostProcessor(
            ConsumerFactory<Object, Object> consumerFactory,
            ActorManager actorManager,
            BufferflyMetrics bufferflyMetrics) {
        return new ShardedActorBeanPostProcessor(consumerFactory, actorManager, bufferflyMetrics);
    }

    /**
     * Registers the {@link BufferflyHealthIndicator} <b>only</b> when Spring Boot
     * Actuator's health API ({@code HealthIndicator}) is on the classpath. Apps
     * without Actuator get no health bean and pull in none of the health module.
     * The indicator is a minimal, stateless {@code isRunning()} aggregate — see
     * its javadoc for why it deliberately checks nothing else.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(org.springframework.boot.health.contributor.HealthIndicator.class)
    static class HealthContributorConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public BufferflyHealthIndicator bufferflyHealthIndicator(
                ShardedActorBeanPostProcessor shardedActorBeanPostProcessor) {
            return new BufferflyHealthIndicator(shardedActorBeanPostProcessor);
        }
    }
}
