package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Auto-configuration entry point for the bufferfly-spring-kafka module.
 *
 * <p>Registers:
 * <ul>
 *   <li>a default {@link ActorManager} (unless the application provides one), and</li>
 *   <li>the {@link ShardedActorBeanPostProcessor} that discovers
 *       {@link ShardedActor}-annotated factory beans and wires each to a
 *       {@link ShardedActorContainer} (one actor per partition).</li>
 * </ul>
 *
 * <p>The {@link ConsumerFactory} is expected to be provided by Spring Boot's
 * Kafka auto-configuration (via {@code spring.kafka.*} properties).
 */
@AutoConfiguration
public class BufferflySpringAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ActorManager bufferflyActorManager() {
        return new ActorManager();
    }

    /**
     * Registered as an infrastructure-role bean. {@code BeanPostProcessor} beans
     * should be as free of dependencies as possible; we take the
     * {@link ConsumerFactory} and {@link ActorManager} as constructor args which
     * Spring resolves eagerly.
     */
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public static ShardedActorBeanPostProcessor shardedActorBeanPostProcessor(
            ConsumerFactory<Object, Object> consumerFactory,
            ActorManager actorManager) {
        return new ShardedActorBeanPostProcessor(consumerFactory, actorManager);
    }
}
