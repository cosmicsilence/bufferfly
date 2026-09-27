package io.bufferfly.core.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link BufferflyConfigLoader}.
 *
 * Covers: properties parsing, YAML parsing, classpath loading,
 * file path loading, defaults, override merging, and error cases.
 */
class BufferflyConfigLoaderTest {

    // -----------------------------------------------------------------------
    // defaults()
    // -----------------------------------------------------------------------

    @Test
    void should_returnBuiltInDefaults_when_noFileProvided() {
        BufferflyConfig config = BufferflyConfigLoader.defaults();

        assertEquals(20_000, config.getActors().getDefaultMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher",
            config.getActors().getDefaultDispatcher());
        assertTrue(config.getActors().getOverrides().isEmpty());
    }

    // -----------------------------------------------------------------------
    // Properties parsing
    // -----------------------------------------------------------------------

    @Test
    void should_parseDefaultCapacityAndDispatcher_when_propertiesFileLoaded(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, """
            bufferfly.actors.default-mailbox-capacity=5000
            bufferfly.actors.default-dispatcher=io.bufferfly.core.actor.VTDispatcher
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);

        assertEquals(5_000, config.getActors().getDefaultMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher",
            config.getActors().getDefaultDispatcher());
    }

    @Test
    void should_parsePerActorOverrides_when_propertiesFileContainsOverrideKeys(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, """
            bufferfly.actors.default-mailbox-capacity=1000
            bufferfly.actors.overrides.payment-actor.mailbox-capacity=500
            bufferfly.actors.overrides.payment-actor.dispatcher=io.bufferfly.core.actor.VTDispatcher
            bufferfly.actors.overrides.audit-actor.mailbox-capacity=200
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);

        Map<String, BufferflyConfig.ActorOverrideConfig> overrides =
            config.getActors().getOverrides();
        assertEquals(2, overrides.size());

        BufferflyConfig.ActorOverrideConfig payment = overrides.get("payment-actor");
        assertNotNull(payment, "payment-actor override must be present");
        assertEquals(500, payment.getMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher", payment.getDispatcher());

        BufferflyConfig.ActorOverrideConfig audit = overrides.get("audit-actor");
        assertNotNull(audit, "audit-actor override must be present");
        assertEquals(200, audit.getMailboxCapacity());
        assertNull(audit.getDispatcher(), "dispatcher not specified — must be null");
    }

    @Test
    void should_useDefaults_when_propertiesFileIsEmpty(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, "# just a comment\n");

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);

        assertEquals(20_000, config.getActors().getDefaultMailboxCapacity());
        assertTrue(config.getActors().getOverrides().isEmpty());
    }

    @Test
    void should_throwConfigException_when_capacityIsNotAnInteger(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, "bufferfly.actors.default-mailbox-capacity=not-a-number\n");

        assertThrows(BufferflyConfigLoader.BufferflyConfigException.class,
            () -> BufferflyConfigLoader.fromPath(file));
    }

    @Test
    void should_throwConfigException_when_fileDoesNotExist(@TempDir Path tmp) {
        Path missing = tmp.resolve("nonexistent.properties");

        assertThrows(BufferflyConfigLoader.BufferflyConfigException.class,
            () -> BufferflyConfigLoader.fromPath(missing));
    }

    // -----------------------------------------------------------------------
    // YAML parsing
    // -----------------------------------------------------------------------

    @Test
    void should_parseDefaultCapacityAndDispatcher_when_yamlFileLoaded(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.yml");
        Files.writeString(file, """
            bufferfly:
              actors:
                default-mailbox-capacity: 750
                default-dispatcher: io.bufferfly.core.actor.VTDispatcher
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);

        assertEquals(750, config.getActors().getDefaultMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher",
            config.getActors().getDefaultDispatcher());
    }

    @Test
    void should_parsePerActorOverrides_when_yamlFileContainsOverrides(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.yml");
        Files.writeString(file, """
            bufferfly:
              actors:
                default-mailbox-capacity: 1000
                overrides:
                  fast-actor:
                    mailbox-capacity: 100
                    dispatcher: io.bufferfly.core.actor.VTDispatcher
                  slow-actor:
                    mailbox-capacity: 50
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);

        assertEquals(1_000, config.getActors().getDefaultMailboxCapacity());

        Map<String, BufferflyConfig.ActorOverrideConfig> overrides =
            config.getActors().getOverrides();
        assertEquals(2, overrides.size());

        assertEquals(100,  overrides.get("fast-actor").getMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher",
            overrides.get("fast-actor").getDispatcher());

        assertEquals(50,   overrides.get("slow-actor").getMailboxCapacity());
        assertNull(overrides.get("slow-actor").getDispatcher());
    }

    @Test
    void should_detectYamlFormat_when_fileHasYmlExtension(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("config.yml");
        Files.writeString(file, """
            bufferfly:
              actors:
                default-mailbox-capacity: 999
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        assertEquals(999, config.getActors().getDefaultMailboxCapacity());
    }

    // -----------------------------------------------------------------------
    // Classpath loading
    // -----------------------------------------------------------------------

    @Test
    void should_loadFromClasspath_when_testPropertiesResourceExists() {
        // bufferfly-test.properties is in src/test/resources — but fromClasspath()
        // looks for "bufferfly.properties". The classpath has the main-resources
        // file (default-mailbox-capacity=20000), so defaults should come through.
        BufferflyConfig config = BufferflyConfigLoader.fromClasspath();

        // classpath has the default bufferfly.properties from main/resources
        assertEquals(20_000, config.getActors().getDefaultMailboxCapacity());
        assertEquals("io.bufferfly.core.actor.VTDispatcher",
            config.getActors().getDefaultDispatcher());
    }

    // -----------------------------------------------------------------------
    // SnakeYAML-backed flatten helper
    // -----------------------------------------------------------------------

    @Test
    void should_flattenNestedYaml_when_flattenCalledOnSnakeYamlMap() {
        // Simulate what SnakeYAML produces for a nested YAML document
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("default-mailbox-capacity", 1234);
        inner.put("default-dispatcher", "com.example.MyDispatcher");

        Map<String, Object> overrideEntry = new LinkedHashMap<>();
        overrideEntry.put("mailbox-capacity", 42);

        Map<String, Object> overrides = new LinkedHashMap<>();
        overrides.put("my-actor", overrideEntry);
        inner.put("overrides", overrides);

        Map<String, Object> actors = new LinkedHashMap<>();
        actors.put("actors", inner);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("bufferfly", actors);

        Map<String, String> flat = new HashMap<>();
        BufferflyConfigLoader.flatten("", root, flat);

        assertEquals("1234",                     flat.get("bufferfly.actors.default-mailbox-capacity"));
        assertEquals("com.example.MyDispatcher", flat.get("bufferfly.actors.default-dispatcher"));
        assertEquals("42",                       flat.get("bufferfly.actors.overrides.my-actor.mailbox-capacity"));
    }

    @Test
    void should_ignoreCommentsAndBlankLines_when_parsingYamlViaSnakeYaml(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.yml");
        Files.writeString(file, """
            # top-level comment
            bufferfly:
              # nested comment
              actors:
                default-mailbox-capacity: 500
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        assertEquals(500, config.getActors().getDefaultMailboxCapacity());
        assertTrue(config.getActors().getOverrides().isEmpty());
    }

    // -----------------------------------------------------------------------
    // Builder / POJO
    // -----------------------------------------------------------------------

    @Test
    void should_buildConfigProgrammatically_when_builderUsedDirectly() {
        BufferflyConfig config = BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder()
                .defaultMailboxCapacity(1_000)
                .defaultDispatcher("io.bufferfly.core.actor.VTDispatcher")
                .addOverride("my-actor", BufferflyConfig.ActorOverrideConfig.builder()
                    .mailboxCapacity(100)
                    .dispatcher("io.bufferfly.core.actor.VTDispatcher")
                    .build())
                .build())
            .build();

        assertEquals(1_000, config.getActors().getDefaultMailboxCapacity());
        assertEquals(1, config.getActors().getOverrides().size());
        assertEquals(100,
            config.getActors().getOverrides().get("my-actor").getMailboxCapacity());
    }
}
