package io.cosmicsilence.bufferfly.core.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Loads a {@link BufferflyConfig} from a {@code bufferfly.properties} or
 * {@code bufferfly.yml} file.
 *
 * <h2>Resolution order</h2>
 * <ol>
 *   <li>Explicit {@link Path} passed to {@link #fromPath(Path)}.</li>
 *   <li>Classpath resource {@code bufferfly.properties} via {@link #fromClasspath()}.</li>
 *   <li>Classpath resource {@code bufferfly.yml} via {@link #fromClasspath()}.</li>
 *   <li>Built-in defaults if neither file is found.</li>
 * </ol>
 *
 * <h2>Properties format</h2>
 * <pre>
 * bufferfly.actors.default-mailbox-capacity=20000
 * bufferfly.actors.default-dispatcher=io.cosmicsilence.bufferfly.core.actor.VTDispatcher
 *
 * bufferfly.actors.overrides.payment-actor.dispatcher=io.acme.PaymentDispatcher
 * bufferfly.actors.overrides.payment-actor.mailbox-capacity=5000
 * bufferfly.actors.overrides.audit-actor.mailbox-capacity=1000
 * </pre>
 *
 * <h2>YAML format</h2>
 * <pre>
 * bufferfly:
 *   actors:
 *     default-mailbox-capacity: 20000
 *     default-dispatcher: io.cosmicsilence.bufferfly.core.actor.VTDispatcher
 *     overrides:
 *       payment-actor:
 *         dispatcher: io.acme.PaymentDispatcher
 *         mailbox-capacity: 5000
 *       audit-actor:
 *         mailbox-capacity: 1000
 * </pre>
 *
 * <h2>Spring compatibility note</h2>
 * Property keys use Spring's relaxed-binding ({@code kebab-case}) conventions.
 * For Spring Boot, bind via:
 * <pre>
 * &#64;ConfigurationProperties("bufferfly")
 * public class BufferflyConfigProperties { ... }
 * </pre>
 * and pass the result to {@link io.cosmicsilence.bufferfly.core.actor.ActorManager#ActorManager(BufferflyConfig)}.
 */
public final class BufferflyConfigLoader {

    private static final String CLASSPATH_PROPERTIES = "bufferfly.properties";
    private static final String CLASSPATH_YAML       = "bufferfly.yml";

    private static final String PREFIX_DEFAULT_CAPACITY   = "bufferfly.actors.default-mailbox-capacity";
    private static final String PREFIX_DEFAULT_DISPATCHER = "bufferfly.actors.default-dispatcher";
    private static final String PREFIX_OVERRIDES          = "bufferfly.actors.overrides.";

    private BufferflyConfigLoader() {}

    // -----------------------------------------------------------------------
    // Public entry points
    // -----------------------------------------------------------------------

    /**
     * Attempts to load from classpath {@code bufferfly.properties}, then
     * {@code bufferfly.yml}, then falls back to built-in defaults.
     */
    public static BufferflyConfig fromClasspath() {
        InputStream props = classpathStream(CLASSPATH_PROPERTIES);
        if (props != null) {
            return fromPropertiesStream(props, CLASSPATH_PROPERTIES);
        }
        InputStream yaml = classpathStream(CLASSPATH_YAML);
        if (yaml != null) {
            return fromYamlStream(yaml);
        }
        return defaults();
    }

    /**
     * Loads from an explicit file path. Detects format by file extension
     * ({@code .properties} or {@code .yml}/{@code .yaml}).
     */
    public static BufferflyConfig fromPath(Path path) {
        if (!Files.exists(path)) {
            throw new BufferflyConfigException("Config file not found: " + path);
        }
        String name = path.getFileName().toString().toLowerCase();
        try (InputStream is = Files.newInputStream(path)) {
            if (name.endsWith(".yml") || name.endsWith(".yaml")) {
                return fromYamlStream(is);
            }
            return fromPropertiesStream(is, path.toString());
        } catch (IOException e) {
            throw new BufferflyConfigException("Failed to read config from: " + path, e);
        }
    }

    /** Returns a config populated entirely from built-in defaults. */
    public static BufferflyConfig defaults() {
        return BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder().build())
            .build();
    }

    // -----------------------------------------------------------------------
    // Properties parsing
    // -----------------------------------------------------------------------

    static BufferflyConfig fromPropertiesStream(InputStream is, String sourceName) {
        Properties props = new Properties();
        try {
            props.load(is);
        } catch (IOException e) {
            throw new BufferflyConfigException("Failed to parse properties from: " + sourceName, e);
        }
        Map<String, String> flat = new HashMap<>();
        props.forEach((k, v) -> flat.put(k.toString().trim(), v.toString().trim()));
        return buildFromFlatMap(flat);
    }

    // -----------------------------------------------------------------------
    // YAML parsing via SnakeYAML
    // -----------------------------------------------------------------------

    static BufferflyConfig fromYamlStream(InputStream is) {
        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(is);
        if (root == null) {
            return defaults();
        }
        Map<String, String> flat = new HashMap<>();
        flatten("", root, flat);
        return buildFromFlatMap(flat);
    }

    /**
     * Recursively flattens a nested {@code Map<String, Object>} produced by
     * SnakeYAML into a dotted-key {@code Map<String, String>}.
     *
     * <p>Example: {@code {bufferfly: {actors: {default-mailbox-capacity: 5000}}}}
     * becomes {@code {"bufferfly.actors.default-mailbox-capacity": "5000"}}.
     */
    @SuppressWarnings("unchecked")
    static void flatten(String prefix, Map<String, Object> map, Map<String, String> result) {
        map.forEach((key, value) -> {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            if (value instanceof Map<?, ?> nested) {
                flatten(fullKey, (Map<String, Object>) nested, result);
            } else if (value != null) {
                result.put(fullKey, value.toString());
            }
        });
    }

    // -----------------------------------------------------------------------
    // Build BufferflyConfig from a flat dotted-key map
    // -----------------------------------------------------------------------

    private static BufferflyConfig buildFromFlatMap(Map<String, String> flat) {
        BufferflyConfig.ActorsConfig.Builder actors = BufferflyConfig.ActorsConfig.builder();

        String rawCapacity = flat.get(PREFIX_DEFAULT_CAPACITY);
        if (rawCapacity != null) {
            actors.defaultMailboxCapacity(parseInt(rawCapacity, PREFIX_DEFAULT_CAPACITY));
        }

        String rawDispatcher = flat.get(PREFIX_DEFAULT_DISPATCHER);
        if (rawDispatcher != null) {
            actors.defaultDispatcher(rawDispatcher);
        }

        // Collect per-actor override keys:
        //   bufferfly.actors.overrides.<actor-name>.dispatcher
        //   bufferfly.actors.overrides.<actor-name>.mailbox-capacity
        Map<String, BufferflyConfig.ActorOverrideConfig.Builder> overrideBuilders = new HashMap<>();

        flat.forEach((key, value) -> {
            if (!key.startsWith(PREFIX_OVERRIDES)) return;

            String rest = key.substring(PREFIX_OVERRIDES.length());
            int dot = rest.indexOf('.');
            if (dot < 0) return;

            String actorName = rest.substring(0, dot);
            String field     = rest.substring(dot + 1);

            BufferflyConfig.ActorOverrideConfig.Builder b =
                overrideBuilders.computeIfAbsent(actorName,
                    ignored -> BufferflyConfig.ActorOverrideConfig.builder());

            switch (field) {
                case "dispatcher"       -> b.dispatcher(value);
                case "mailbox-capacity" -> b.mailboxCapacity(parseInt(value, key));
                default -> { /* unknown field — ignore for forward compatibility */ }
            }
        });

        overrideBuilders.forEach((name, b) -> actors.addOverride(name, b.build()));

        return BufferflyConfig.builder().actors(actors.build()).build();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static InputStream classpathStream(String resource) {
        InputStream is = Thread.currentThread().getContextClassLoader()
                               .getResourceAsStream(resource);
        if (is == null) {
            is = BufferflyConfigLoader.class.getClassLoader().getResourceAsStream(resource);
        }
        return is;
    }

    private static int parseInt(String value, String key) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new BufferflyConfigException(
                "Invalid integer value for key '%s': '%s'".formatted(key, value));
        }
    }

    // -----------------------------------------------------------------------
    // Exception
    // -----------------------------------------------------------------------

    public static final class BufferflyConfigException extends RuntimeException {
        public BufferflyConfigException(String message) {
            super(message);
        }
        public BufferflyConfigException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
