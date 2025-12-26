package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.test.support.TestConfig;

@Tag("driver")
@Tag("morphiumserver_skip")  // Connection pool behavior differs with MorphiumServer
public class PooledDriverConnectionsTests {
    private Logger log = LoggerFactory.getLogger(PooledDriverConnectionsTests.class);

    @Test
    public void testLotsConnectionPool() throws Exception {
        if (TestConfig.load().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            return;
        }
        var drv = getDriver();
        drv.setMaxWaitTime(2000);
        drv.setHeartbeatFrequency(250);

        drv.connect();
        log.info("MaxWaitTime is          : " + drv.getMaxWaitTime());
        log.info("idle time is            : " + drv.getIdleSleepTime());
        log.info("maxConnectionLifeTime is: " + drv.getMaxConnectionLifetime());
        log.info("maxConnectionIdleTime is: " + drv.getMaxConnectionIdleTime());
        log.info("setMaxConnectionsPerHost: " + drv.getMaxConnectionsPerHost());

        while (!drv.isConnected()) {
            Thread.sleep(100);
        }

        Thread.sleep(2000);
        log.info("connected...");

        for (var e : drv.getNumConnectionsByHost().entrySet()) {
            assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
        }

        log.info("Properly connected...");
        // for (int i = 0; i < 15; i++) {
        //     var m = new HashMap<String, Integer>(drv.getNumConnectionsByHost());
        //     log.info("Checking connection pool...");
        //
        //     for (var e : m.entrySet()) {
        //         assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
        //         log.info("Connections to " + e.getKey() + ": " + e.getValue());
        //     }
        //
        //     log.info(".. done");
        //     Thread.sleep(1000);
        // }
        log.info("Getting c1");
        var c1 = drv.getPrimaryConnection(null);
        log.info("Getting c2");
        var c2 = drv.getPrimaryConnection(null);
        log.info("Getting c3");
        var c3 = drv.getPrimaryConnection(null);
        log.info("Getting c4");
        var c4 = drv.getPrimaryConnection(null);
        log.info("Got all connections....");
        assertEquals(4, drv.getNumConnectionsByHost().get(c4.getConnectedTo()));
        drv.releaseConnection(c1);
        drv.releaseConnection(c2);
        drv.releaseConnection(c3);
        drv.releaseConnection(c4);
        assertEquals(4, drv.getNumConnectionsByHost().get(c4.getConnectedTo()));
        log.info("All released");
        List<MongoConnection> lst = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            try {
                log.info("Processing {}", i);
                var c = drv.getPrimaryConnection(null);
                lst.add(c);

            } catch (Exception e) {
                log.info("could not get connection #{} - got {}", i, lst.size());
            }
        }

        log.info("connections {}", lst.size());

        for (MongoConnection c : lst) {
            drv.releaseConnection(c);
        }

        log.info("Waiting for pool to be cleared afer idleTime...");


        for (int i = 0; i < drv.getMaxConnectionLifetime() / 1000 + 3; i++) {
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

        log.info("Statistics: ");

        for (var k : drv.getDriverStats().keySet()) {
            log.info("{} -> {}", k, drv.getDriverStats().get(k));
        }


        drv.close();
    }

    @Test
    public void testConnectionPool() throws Exception {
        if (TestConfig.load().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            return;
        }
        var drv = getDriver();
        drv.connect();
        log.info("MaxWaitTime is: " + drv.getMaxWaitTime());
        log.info("idle time is: " + drv.getIdleSleepTime());
        log.info("maxConnectionLifeTime is: " + drv.getMaxConnectionLifetime());
        log.info("maxConnectionIdleTime is: " + drv.getMaxConnectionIdleTime());
        log.info("setMaxConnectionsPerHost: " + drv.getMaxConnectionsPerHost());

        while (!drv.isConnected()) {
            Thread.sleep(100);
        }

        Thread.sleep(2000);
        log.info("connected...");

        for (var e : drv.getNumConnectionsByHost().entrySet()) {
            assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
        }

        log.info("Properly connected...");

        int error = 0;
        for (int i = 0; i < drv.getMaxConnectionLifetime() / 1000 + 2; i++) {
            var m = new HashMap<String, Integer>(drv.getNumConnectionsByHost());
            log.info("Checking connection pool...");

            for (var e : m.entrySet()) {
                if (e.getValue() != 2) error++;
                // assertEquals(2, e.getValue(), "num connections to " + e.getKey() + " wrong!");
                log.info("Connections to " + e.getKey() + ": " + e.getValue());
            }

            log.info(".. done");
            log.info("Errors {}", error);
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

        for (int i = 0; i < drv.getMaxConnectionIdleTime() / 1000 + 3; i++) {
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
        MorphiumConfig cfg = TestConfig.load();
        var drv = new PooledDriver();

        // Host seed
        var seeds = cfg.clusterSettings().getHostSeed();
        if (seeds != null && !seeds.isEmpty()) {
            drv.setHostSeed(seeds.toArray(new String[0]));
        }

        // Credentials (only if provided)
        var user = cfg.authSettings().getMongoLogin();
        var pwd = cfg.authSettings().getMongoPassword();
        var authDb = cfg.authSettings().getMongoAuthDb();
        if (user != null && pwd != null) {
            drv.setCredentials(authDb != null ? authDb : "admin", user, pwd);
        }

        // Pool and timeouts based on test config, with minimal overrides for test expectations
        drv.setMaxConnections(cfg.connectionSettings().getMaxConnections());
        drv.setMinConnections(cfg.connectionSettings().getMinConnections());
        drv.setConnectionTimeout(cfg.connectionSettings().getConnectionTimeout());
        // drv.setMaxConnectionLifetime(cfg.driverSettings().getMaxConnectionLifeTime());
        drv.setMaxConnectionLifetime(1000);
        // drv.setMaxConnectionIdleTime(cfg.driverSettings().getMaxConnectionIdleTime());
        drv.setMaxConnectionIdleTime(5000);
        drv.setDefaultReadPreference(cfg.driverSettings().getDefaultReadPreference() != null
                                     ? cfg.driverSettings().getDefaultReadPreference()
                                     : ReadPreference.nearest());
        drv.setHeartbeatFrequency(cfg.driverSettings().getHeartbeatFrequency());

        // Ensure min connections per host match assertions in this test
        drv.setMinConnectionsPerHost(2);
        return drv;
    }
}
