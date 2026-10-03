package io.bufferfly.core.config;

import io.bufferfly.core.actor.ActorManager;
import io.bufferfly.core.actor.ActorReference;
import io.bufferfly.core.actor.DummyCountingActor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying that {@link BufferflyConfig} is correctly wired
 * into {@link ActorManager} — i.e. the config values actually govern dispatch
 * behaviour end-to-end.
 */
@Timeout(30)
class ActorManagerConfigTest {

    // -----------------------------------------------------------------------
    // Default constructor (no config) — baseline
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessage_when_actorManagerUsesDefaultConstructor() {
        ActorManager manager = new ActorManager();
        DummyCountingActor actor = new DummyCountingActor("default-actor");

        ActorReference<String> ref = manager.start(actor);
        ref.tell("hello");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));
    }

    // -----------------------------------------------------------------------
    // Config constructor — built-in defaults
    // -----------------------------------------------------------------------

    @Test
    void should_deliverMessage_when_actorManagerWiredWithDefaultConfig() {
        BufferflyConfig config = BufferflyConfigLoader.defaults();
        ActorManager manager  = new ActorManager(config);
        DummyCountingActor actor = new DummyCountingActor("config-default-actor");

        ActorReference<String> ref = manager.start(actor);
        ref.tell("ping");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));
    }

    // -----------------------------------------------------------------------
    // Config from properties file
    // -----------------------------------------------------------------------

    @Test
    void should_respectCustomDefaultCapacity_when_configLoadedFromPropertiesFile(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, """
            bufferfly.actors.default-mailbox-capacity=10
            bufferfly.actors.default-dispatcher=io.bufferfly.core.actor.VTDispatcher
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        assertEquals(10, config.getActors().getDefaultMailboxCapacity());

        ActorManager manager = new ActorManager(config);
        DummyCountingActor actor = new DummyCountingActor("small-mailbox-actor");
        ActorReference<String> ref = manager.start(actor);

        for (int i = 0; i < 5; i++) ref.tell("msg-" + i);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(5, actor.getReceiveCount()));
    }

    @Test
    void should_wirePerActorCapacityOverride_when_configContainsOverrideForActor(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.properties");
        Files.writeString(file, """
            bufferfly.actors.default-mailbox-capacity=20000
            bufferfly.actors.default-dispatcher=io.bufferfly.core.actor.VTDispatcher
            bufferfly.actors.overrides.special-actor.mailbox-capacity=50
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        ActorManager manager = new ActorManager(config);

        // Verify the override was parsed
        assertEquals(50,
            config.getActors().getOverrides().get("special-actor").getMailboxCapacity());

        // Actor still receives messages — capacity only restricts burst, not correctness
        DummyCountingActor actor = new DummyCountingActor("special-actor");
        ActorReference<String> ref = manager.start(actor);

        for (int i = 0; i < 10; i++) ref.tell("msg-" + i);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(10, actor.getReceiveCount()));
    }

    // -----------------------------------------------------------------------
    // Config from YAML file
    // -----------------------------------------------------------------------

    @Test
    void should_respectCustomDefaultCapacity_when_configLoadedFromYamlFile(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.yml");
        Files.writeString(file, """
            bufferfly:
              actors:
                default-mailbox-capacity: 100
                default-dispatcher: io.bufferfly.core.actor.VTDispatcher
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        assertEquals(100, config.getActors().getDefaultMailboxCapacity());

        ActorManager manager = new ActorManager(config);
        DummyCountingActor actor = new DummyCountingActor("yaml-actor");
        ActorReference<String> ref = manager.start(actor);

        ref.tell("from-yaml-config");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));
    }

    @Test
    void should_wirePerActorOverride_when_yamlConfigContainsOverrides(@TempDir Path tmp)
            throws IOException {
        Path file = tmp.resolve("bufferfly.yml");
        Files.writeString(file, """
            bufferfly:
              actors:
                default-mailbox-capacity: 20000
                overrides:
                  priority-actor:
                    mailbox-capacity: 500
                  bulk-actor:
                    mailbox-capacity: 5000
            """);

        BufferflyConfig config = BufferflyConfigLoader.fromPath(file);
        ActorManager manager  = new ActorManager(config);

        DummyCountingActor priority = new DummyCountingActor("priority-actor");
        DummyCountingActor bulk     = new DummyCountingActor("bulk-actor");

        ActorReference<String> refP = manager.start(priority);
        ActorReference<String> refB = manager.start(bulk);

        for (int i = 0; i < 20; i++) {
            refP.tell("p-" + i);
            refB.tell("b-" + i);
        }

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   assertEquals(20, priority.getReceiveCount());
                   assertEquals(20, bulk.getReceiveCount());
               });
    }

    // -----------------------------------------------------------------------
    // Dispatcher FQN wiring
    // -----------------------------------------------------------------------

    @Test
    void should_instantiateVTDispatcher_when_defaultDispatcherFqnIsVTDispatcher() {
        BufferflyConfig config = BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder()
                .defaultDispatcher("io.bufferfly.core.actor.VTDispatcher")
                .defaultMailboxCapacity(1_000)
                .build())
            .build();

        // Should not throw
        ActorManager manager = new ActorManager(config);
        DummyCountingActor actor = new DummyCountingActor("fqn-actor");
        ActorReference<String> ref = manager.start(actor);
        ref.tell("test");

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(1, actor.getReceiveCount()));
    }

    @Test
    void should_throwIllegalArgument_when_defaultDispatcherFqnNotFound() {
        BufferflyConfig config = BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder()
                .defaultDispatcher("com.nonexistent.CustomDispatcher")
                .build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> new ActorManager(config));
    }

    @Test
    void should_throwIllegalArgument_when_dispatcherFqnDoesNotImplementDispatcher() {
        BufferflyConfig config = BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder()
                // A real class that does NOT implement Dispatcher
                .defaultDispatcher("java.lang.String")
                .build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> new ActorManager(config));
    }

    // -----------------------------------------------------------------------
    // Programmatic config + full delivery
    // -----------------------------------------------------------------------

    @Test
    void should_deliverBurstMessages_when_actorManagerWiredProgrammatically() {
        final int BURST = 500;
        BufferflyConfig config = BufferflyConfig.builder()
            .actors(BufferflyConfig.ActorsConfig.builder()
                .defaultMailboxCapacity(BURST + 100)
                .addOverride("burst-actor", BufferflyConfig.ActorOverrideConfig.builder()
                    .mailboxCapacity(BURST + 100)
                    .build())
                .build())
            .build();

        ActorManager manager = new ActorManager(config);
        DummyCountingActor actor = new DummyCountingActor("burst-actor");
        ActorReference<String> ref = manager.start(actor);

        for (int i = 0; i < BURST; i++) ref.tell("msg-" + i);

        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertEquals(BURST, actor.getReceiveCount()));
    }
}
