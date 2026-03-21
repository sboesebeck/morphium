package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the static driver and messaging registries in {@link Morphium}
 * that make ClassGraph optional for resolving built-in implementations.
 */
@Tag("core")
class DriverRegistryTest {

    // ------------------------------------------------------------------
    // Driver registry
    // ------------------------------------------------------------------

    @Test
    void inMemoryDriver_resolvedFromRegistry() {
        MorphiumConfig cfg = new MorphiumConfig("driver_registry_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        try (Morphium m = new Morphium(cfg)) {
            assertInstanceOf(InMemoryDriver.class, m.getDriver());
        }
    }

    @Test
    void registerCustomDriver_isUsable() {
        // Use a unique name to avoid cross-test coupling in the static registry
        String uniqueName = "CustomTestDriver_" + System.nanoTime();
        Morphium.registerDriver(uniqueName, InMemoryDriver.class);
        MorphiumConfig cfg = new MorphiumConfig("custom_driver_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(uniqueName);
        try (Morphium m = new Morphium(cfg)) {
            assertInstanceOf(InMemoryDriver.class, m.getDriver());
        }
    }

    @Test
    void registerDriver_nullName_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
            Morphium.registerDriver(null, InMemoryDriver.class));
    }

    @Test
    void registerDriver_nullClass_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
            Morphium.registerDriver("test", null));
    }

    // ------------------------------------------------------------------
    // Messaging registry
    // ------------------------------------------------------------------

    @Test
    void builtInMessaging_standardMessaging_resolves() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("messaging_registry_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        cfg.messagingSettings().setMessagingImplementation("StandardMessaging");
        try (Morphium m = new Morphium(cfg)) {
            var messaging = m.createMessaging();
            assertInstanceOf(SingleCollectionMessaging.class, messaging);
            messaging.terminate();
        }
    }

    @Test
    void registerMessaging_nullName_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
            Morphium.registerMessaging(null, SingleCollectionMessaging.class));
    }

    @Test
    void registerMessaging_nullClass_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
            Morphium.registerMessaging("test", null));
    }

    @Test
    void registerCustomMessaging_isAccepted() {
        String uniqueName = "CustomMessaging_" + System.nanoTime();
        assertDoesNotThrow(() ->
            Morphium.registerMessaging(uniqueName, SingleCollectionMessaging.class));
    }
}
