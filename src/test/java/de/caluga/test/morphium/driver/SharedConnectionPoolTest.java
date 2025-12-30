package de.caluga.test.morphium.driver;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
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
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void testSharedConnectionPoolEnabled(Morphium morphium) throws InterruptedException  {

        var cfg1 = morphium.getConfig().createCopy();
        cfg1.driverSettings().setSharedConnectionPool(true);

        var cfg2 = morphium.getConfig().createCopy();
        cfg2.driverSettings().setSharedConnectionPool(true);
        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            log.info("Driver 1 hashcode: {}", System.identityHashCode(m1.getDriver()));
            log.info("Driver 2 hashcode: {}", System.identityHashCode(m2.getDriver()));
            assertSame(m1.getDriver(), m2.getDriver(),
                       "Morphium instances with same hosts+db and sharedConnectionPool=true should share driver instance: " + m1.getDriver().getName() + "/" + m2.getDriver().getName());

        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void testSharedConnectionPoolDifferentDatabases(Morphium morphium) throws InterruptedException  {
        var cfg1 = morphium.getConfig().createCopy();
        var cfg2 = morphium.getConfig().createCopy();
        cfg1.driverSettings().setSharedConnectionPool(true);
        cfg1.driverSettings().setInMemorySharedDatabases(true);
        cfg1.connectionSettings().setDatabase("test_db_1");

        cfg2.driverSettings().setSharedConnectionPool(true);
        cfg2.connectionSettings().setDatabase("test_db_2");
        cfg2.driverSettings().setInMemorySharedDatabases(true);

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            log.info("Driver 1 hashcode: {}", System.identityHashCode(m1.getDriver()));
            log.info("Driver 2 hashcode: {}", System.identityHashCode(m2.getDriver()));
            assertNotSame(m1.getDriver(), m2.getDriver(),
                          "Morphium instances with different databases should NOT share driver even with sharedConnectionPool=true");

        }
    }
}
