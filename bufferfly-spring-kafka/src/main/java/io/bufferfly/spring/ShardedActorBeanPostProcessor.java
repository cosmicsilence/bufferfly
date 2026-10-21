package io.bufferfly.spring;

import io.bufferfly.core.actor.ActorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link BeanPostProcessor} that discovers {@link ShardedActor}-annotated beans
 * and wires each one to a {@link ShardedActorContainer}.
 *
 * <p>During bean initialisation it <em>collects</em> every bean annotated with
 * {@link ShardedActor} that also implements {@link ShardedActorFactory}, reading
 * the annotation's attributes (name, topic, concurrency, high/low watermarks).
 * It does not start anything yet — containers are built and started once in
 * {@link #afterSingletonsInstantiated()}, after the whole context (including the
 * {@link ConsumerFactory} and {@link ActorManager}) is ready.
 *
 * <p>On context shutdown ({@link #destroy()}) all containers are stopped, which
 * stops their per-partition actors.
 */
public class ShardedActorBeanPostProcessor
        implements BeanPostProcessor, SmartInitializingSingleton, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ShardedActorBeanPostProcessor.class);

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final ActorManager actorManager;

    private final List<Discovered> discovered = new ArrayList<>();
    private final List<ShardedActorContainer<Object, Object>> containers = new ArrayList<>();

    private record Discovered(ShardedActor annotation, ShardedActorFactory<Object> factory, String beanName) {
    }

    public ShardedActorBeanPostProcessor(ConsumerFactory<Object, Object> consumerFactory,
                                         ActorManager actorManager) {
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.actorManager = Objects.requireNonNull(actorManager, "actorManager");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ShardedActor annotation = AnnotationUtils.findAnnotation(bean.getClass(), ShardedActor.class);
        if (annotation == null) {
            return bean;
        }
        if (!(bean instanceof ShardedActorFactory<?> factory)) {
            throw new IllegalStateException(
                    "Bean '%s' is annotated with @ShardedActor but does not implement ShardedActorFactory"
                            .formatted(beanName));
        }
        validate(annotation, beanName);
        discovered.add(new Discovered(annotation, (ShardedActorFactory<Object>) factory, beanName));
        log.info("Discovered @ShardedActor bean '{}' (name={}, topic={}, concurrency={}, high={}, low={})",
                beanName, annotation.name(), annotation.topic(), annotation.concurrency(),
                annotation.highWatermark(), annotation.lowWatermark());
        return bean;
    }

    private static void validate(ShardedActor a, String beanName) {
        if (a.name().isBlank())
            throw new IllegalStateException("@ShardedActor.name() must not be blank on bean '" + beanName + "'");
        if (a.topic().isBlank())
            throw new IllegalStateException("@ShardedActor.topic() must not be blank on bean '" + beanName + "'");
        if (a.concurrency() < 1)
            throw new IllegalStateException("@ShardedActor.concurrency() must be >= 1 on bean '" + beanName + "'");
        if (a.highWatermark() <= a.lowWatermark())
            throw new IllegalStateException(
                    "@ShardedActor.highWatermark() (%d) must be > lowWatermark() (%d) on bean '%s'"
                            .formatted(a.highWatermark(), a.lowWatermark(), beanName));
        if (a.lowWatermark() < 0)
            throw new IllegalStateException("@ShardedActor.lowWatermark() must be >= 0 on bean '" + beanName + "'");
    }

    @Override
    public void afterSingletonsInstantiated() {
        buildContainers();
        startContainers();
    }

    /** Builds (but does not start) one container per discovered factory. */
    void buildContainers() {
        for (Discovered d : discovered) {
            ShardedActorContainer<Object, Object> container = new ShardedActorContainer<>(
                    d.annotation().name(),
                    d.annotation().topic(),
                    d.annotation().concurrency(),
                    d.factory(),
                    consumerFactory,
                    actorManager);
            containers.add(container);
        }
    }

    /** Starts every built container. */
    void startContainers() {
        containers.forEach(ShardedActorContainer::start);
        log.info("Started {} sharded actor container(s)", containers.size());
    }

    @Override
    public void destroy() {
        for (ShardedActorContainer<Object, Object> c : containers) {
            try {
                c.stop();
            } catch (Exception e) {
                log.warn("Error stopping sharded actor container '{}'", c.name(), e);
            }
        }
    }

    /** Test/inspection accessor. */
    List<ShardedActorContainer<Object, Object>> containers() {
        return containers;
    }
}
