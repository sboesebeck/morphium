package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.wire.PooledDriver;

public class PooledDriverConnectionsTests {
    private Logger log = LoggerFactory.getLogger(PooledDriverConnectionsTests.class);

    @Test
    public void testConnectionPool() throws Exception {
        var drv = getDriver();
        drv.connect();

        while (!drv.isConnected()) {
            Thread.sleep(100);
        }

        Thread.sleep(2000);
        log.info("connected...");

        for (var e : drv.getNumConnectionsByHost().entrySet()) {
            assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
        }

        log.info("Properly connected...");

        for (int i = 0; i < 20; i++) {
            for (var e : drv.getNumConnectionsByHost().entrySet()) {
                // assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
                log.info("Connections to "+e.getKey()+": "+e.getValue());
            }
            Thread.sleep(1000);
        }

        drv.close();
    }

    private PooledDriver getDriver() throws MorphiumDriverException {
        var drv = new PooledDriver();
        drv.setCredentials("admin", "test", "test");
        drv.setHostSeed("127.0.0.1:27018", "127.0.0.1:27017", "127.0.0.1:27019");
        drv.setMaxConnectionsPerHost(10);
        drv.setHeartbeatFrequency(500);
        drv.setMinConnectionsPerHost(2);
        drv.setMinConnections(2);
        drv.setConnectionTimeout(5000);
        drv.setMaxConnectionLifetime(30000);
        drv.setMaxConnectionIdleTime(10000);
        drv.setDefaultReadPreference(ReadPreference.nearest());
        drv.setHeartbeatFrequency(1000);
        return drv;
    }
}
