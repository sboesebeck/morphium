package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.HashMap;
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

        for (int i = 0; i < 15; i++) {
            var m = new HashMap<String, Integer>(drv.getNumConnectionsByHost());
            log.info("Checking connection pool...");
            for (var e : m.entrySet()) {
                assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
                log.info("Connections to " + e.getKey() + ": " + e.getValue());
            }
            log.info(".. done");

            Thread.sleep(1000);
        }

        var c1 = drv.getPrimaryConnection(null);
        var c2 = drv.getPrimaryConnection(null);
        var c3 = drv.getPrimaryConnection(null);
        var c4 = drv.getPrimaryConnection(null);
        assertEquals(4, drv.getNumConnectionsByHost().get(c4.getConnectedTo()));
        drv.releaseConnection(c1);
        drv.releaseConnection(c2);
        drv.releaseConnection(c3);
        drv.releaseConnection(c4);
        assertEquals(4, drv.getNumConnectionsByHost().get(c4.getConnectedTo()));
        log.info("Waiting for pool to be cleared...");

        for (int i = 0; i < 13; i++) {
            var m = new HashMap<String, Integer>(drv.getNumConnectionsByHost());

            for (var e : m.entrySet()) {
                log.info("Connections to " + e.getKey() + ": " + e.getValue());
            }

            Thread.sleep(1000);
        }

        var m = new HashMap<String, Integer>(drv.getNumConnectionsByHost());

        for (var e : m.entrySet()) {
            assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
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
        drv.setMaxConnectionLifetime(15000);
        drv.setMaxConnectionIdleTime(10000);
        drv.setDefaultReadPreference(ReadPreference.nearest());
        drv.setHeartbeatFrequency(1000);
        return drv;
    }
}
