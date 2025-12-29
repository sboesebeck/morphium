package de.caluga.test.morphium.driver;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.support.TestConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for shared connection pool feature.
 * When sharedConnectionPool is enabled, multiple Morphium instances connecting
 * to the same hosts+database will share the same driver instance.
 */
public class SharedConnectionPoolTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSharedConnectionPoolEnabled(Morphium morphium) throws InterruptedException  {
        // Load base config from test environment
        MorphiumConfig cfg1 = TestConfig.load();
        if (cfg1.driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Cannot test driver share on inMemoryDriver");
            return;
        }
        cfg1.driverSettings().setSharedConnectionPool(true);

        MorphiumConfig cfg2 = TestConfig.load();
        cfg2.driverSettings().setSharedConnectionPool(true);
        cfg2.driverSettings().setInMemorySharedDatabases(true);

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertSame(m1.getDriver(), m2.getDriver(),
                       "Morphium instances with same hosts+db and sharedConnectionPool=true should share driver instance");

            log.info("Driver 1 hashcode: {}", System.identityHashCode(m1.getDriver()));
            log.info("Driver 2 hashcode: {}", System.identityHashCode(m2.getDriver()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSharedConnectionPoolDisabledByDefault(Morphium morphium) throws InterruptedException  {
        // Load base config from test environment
        MorphiumConfig cfg1 = TestConfig.load();
        if (cfg1.driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Cannot test driver share on inMemoryDriver");
            return;
        }
        // sharedConnectionPool is false by default

        MorphiumConfig cfg2 = TestConfig.load();
        // sharedConnectionPool is false by default

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertNotSame(m1.getDriver(), m2.getDriver(),
                          "Morphium instances with sharedConnectionPool=false (default) should NOT share driver instance");

            log.info("Driver 1 hashcode: {}", System.identityHashCode(m1.getDriver()));
            log.info("Driver 2 hashcode: {}", System.identityHashCode(m2.getDriver()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSharedConnectionPoolDifferentDatabases(Morphium morphium) throws InterruptedException  {
        // Load base config from test environment
        MorphiumConfig cfg1 = TestConfig.load();
        if (cfg1.driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Cannot test driver share on inMemoryDriver");
            return;
        }
        cfg1.driverSettings().setSharedConnectionPool(true);
        cfg1.driverSettings().setInMemorySharedDatabases(true);
        cfg1.connectionSettings().setDatabase("test_db_1");

        MorphiumConfig cfg2 = TestConfig.load();
        cfg2.driverSettings().setSharedConnectionPool(true);
        cfg2.connectionSettings().setDatabase("test_db_2");
        cfg2.driverSettings().setInMemorySharedDatabases(true);

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertNotSame(m1.getDriver(), m2.getDriver(),
                          "Morphium instances with different databases should NOT share driver even with sharedConnectionPool=true");

            log.info("Driver 1 hashcode: {}", System.identityHashCode(m1.getDriver()));
            log.info("Driver 2 hashcode: {}", System.identityHashCode(m2.getDriver()));
        }
    }
}
