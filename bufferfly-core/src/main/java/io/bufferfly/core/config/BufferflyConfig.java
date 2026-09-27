package io.bufferfly.core.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Root configuration POJO for the BufferFly actor framework.
 *
 * <p>Property prefix: {@code bufferfly}
 *
 * <p>This class is intentionally framework-agnostic so it can be populated:
 * <ul>
 *   <li>Programmatically via the fluent builder.</li>
 *   <li>From a {@code bufferfly.properties} or {@code bufferfly.yml} file via
 *       {@link io.bufferfly.core.config.BufferflyConfigLoader}.</li>
 *   <li>From Spring Boot's {@code application.properties} / {@code application.yml}
 *       by annotating a subclass or wrapper with
 *       {@code @ConfigurationProperties("bufferfly")} — the field names and nesting
 *       deliberately mirror Spring's relaxed-binding conventions.</li>
 * </ul>
 *
 * <h2>Example {@code bufferfly.properties}</h2>
 * <pre>
 * bufferfly.actors.default-mailbox-capacity=20000
 * bufferfly.actors.default-dispatcher=io.bufferfly.core.actor.VTDispatcher
 *
 * # Per-actor overrides (actor name used as key)
 * bufferfly.actors.overrides.payment-actor.dispatcher=io.bufferfly.core.actor.VTDispatcher
 * bufferfly.actors.overrides.payment-actor.mailbox-capacity=5000
 * </pre>
 *
 * <h2>Example {@code bufferfly.yml}</h2>
 * <pre>
 * bufferfly:
 *   actors:
 *     default-mailbox-capacity: 20000
 *     default-dispatcher: io.bufferfly.core.actor.VTDispatcher
 *     overrides:
 *       payment-actor:
 *         dispatcher: io.bufferfly.core.actor.VTDispatcher
 *         mailbox-capacity: 5000
 * </pre>
 */
public final class BufferflyConfig {

    private final ActorsConfig actors;

    private BufferflyConfig(ActorsConfig actors) {
        this.actors = actors;
    }

    public ActorsConfig getActors() {
        return actors;
    }

    // -----------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ActorsConfig actors = new ActorsConfig.Builder().build();

        public Builder actors(ActorsConfig actors) {
            this.actors = actors;
            return this;
        }

        public BufferflyConfig build() {
            return new BufferflyConfig(actors);
        }
    }

    // -----------------------------------------------------------------------
    // Nested: actors pool settings
    // -----------------------------------------------------------------------

    /**
     * Settings under {@code bufferfly.actors}.
     */
    public static final class ActorsConfig {

        /**
         * Default mailbox capacity for all actors that have no per-actor override.
         */
        private final int defaultMailboxCapacity;

        /**
         * Fully-qualified class name of the default {@link io.bufferfly.core.actor.Dispatcher}
         * implementation to use when no per-actor override is present.
         * Must have a public no-arg constructor.
         */
        private final String defaultDispatcher;

        /**
         * Per-actor overrides, keyed by actor name.
         */
        private final Map<String, ActorOverrideConfig> overrides;

        private ActorsConfig(Builder b) {
            this.defaultMailboxCapacity = b.defaultMailboxCapacity;
            this.defaultDispatcher = b.defaultDispatcher;
            this.overrides = Collections.unmodifiableMap(new HashMap<>(b.overrides));
        }

        public int getDefaultMailboxCapacity() {
            return defaultMailboxCapacity;
        }

        public String getDefaultDispatcher() {
            return defaultDispatcher;
        }

        public Map<String, ActorOverrideConfig> getOverrides() {
            return overrides;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private int defaultMailboxCapacity = 20_000;
            private String defaultDispatcher = "io.bufferfly.core.actor.VTDispatcher";
            private final Map<String, ActorOverrideConfig> overrides = new HashMap<>();

            public Builder defaultMailboxCapacity(int capacity) {
                this.defaultMailboxCapacity = capacity;
                return this;
            }

            public Builder defaultDispatcher(String fqn) {
                this.defaultDispatcher = fqn;
                return this;
            }

            public Builder addOverride(String actorName, ActorOverrideConfig override) {
                this.overrides.put(actorName, override);
                return this;
            }

            public ActorsConfig build() {
                return new ActorsConfig(this);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Nested: per-actor override
    // -----------------------------------------------------------------------

    /**
     * Per-actor overrides under {@code bufferfly.actors.overrides.<actor-name>}.
     */
    public static final class ActorOverrideConfig {

        /**
         * FQN of the {@link io.bufferfly.core.actor.Dispatcher} to use for this actor.
         * {@code null} means fall back to the pool default.
         */
        private final String dispatcher;

        /**
         * Mailbox capacity for this actor.
         * {@code -1} means fall back to the pool default.
         */
        private final int mailboxCapacity;

        private ActorOverrideConfig(Builder b) {
            this.dispatcher = b.dispatcher;
            this.mailboxCapacity = b.mailboxCapacity;
        }

        public String getDispatcher() {
            return dispatcher;
        }

        public int getMailboxCapacity() {
            return mailboxCapacity;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String dispatcher = null;
            private int mailboxCapacity = -1;

            public Builder dispatcher(String fqn) {
                this.dispatcher = fqn;
                return this;
            }

            public Builder mailboxCapacity(int capacity) {
                this.mailboxCapacity = capacity;
                return this;
            }

            public ActorOverrideConfig build() {
                return new ActorOverrideConfig(this);
            }
        }
    }
}
