package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that built-in drivers can be resolved via the static driver registry
 * without ClassGraph.
 */
@Tag("core")
class DriverRegistryTest {

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
}
