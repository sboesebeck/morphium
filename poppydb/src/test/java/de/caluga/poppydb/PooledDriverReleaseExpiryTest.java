package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.PooledDriver;

/**
 * Connections released back to the pool after exceeding their maxConnectionLifetime (or idle
 * time) used to be pooled anyway - only the heartbeat's expiry sweep removed them one
 * heartbeat later. A borrow burst (e.g. 20 connections) thus parked a mountain of
 * already-expired connections in the pool, and under load the sweep lagged behind, keeping
 * the pool far above its per-host minimum for many seconds (the
 * PooledDriverConnectionsTests.testLotsConnectionPool flaky). releaseConnection must close
 * expired connections instead of pooling them - like the official MongoDB drivers do.
 */
public class PooledDriverReleaseExpiryTest {

    private PoppyDB server;
    private PooledDriver drv;

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @AfterEach
    void tearDown() {
        if (drv != null) {
            try {
                drv.close();
            } catch (Exception ignored) {
            }
        }

        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    void expiredConnectionsAreClosedOnReleaseNotPooled() throws Exception {
        int port = freePort();
        server = new PoppyDB(port, "127.0.0.1", 100, 60);
        server.start();
        Thread.sleep(500);

        drv = new PooledDriver();
        drv.setHostSeed("127.0.0.1:" + port);
        drv.setMaxConnections(100);
        drv.setMinConnectionsPerHost(2);
        drv.setConnectionTimeout(2000);
        drv.setMaxWaitTime(2000);
        drv.setMaxConnectionLifetime(500);
        drv.setMaxConnectionIdleTime(10_000);
        // slow heartbeat so ITS expiry sweep cannot clean up for us - the release path
        // itself must refuse to pool the expired connections
        drv.setHeartbeatFrequency(5_000);
        drv.connect();

        long deadline = System.currentTimeMillis() + 5_000;

        while (!drv.isConnected() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        List<MongoConnection> held = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            held.add(drv.getPrimaryConnection(null));
        }

        // hold them beyond their lifetime
        Thread.sleep(800);

        for (MongoConnection c : held) {
            drv.releaseConnection(c);
        }

        Map<String, Integer> conns = drv.getNumConnectionsByHost();
        int total = conns.values().stream().mapToInt(Integer::intValue).sum();
        assertTrue(total <= 2,
            "expired connections must be closed on release, not returned to the pool - got " + conns);
    }
}
