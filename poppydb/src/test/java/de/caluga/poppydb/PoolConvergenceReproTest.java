package de.caluga.poppydb;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.PooledDriver;

/**
 * Observational repro for the PooledDriverConnectionsTests.testLotsConnectionPool flaky
 * (pool reported {primary=20} instead of converging to minConnectionsPerHost=2 after the
 * 20-borrow/release burst). Mirrors the failing test's configuration exactly and samples
 * every pool counter during the convergence window. Not an assertion test - it prints the
 * counter timeline for analysis.
 */
@Disabled("diagnostic repro - run manually")
public class PoolConvergenceReproTest {

    private PoppyDB server;

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.shutdown();
        }

        if (rsServers != null) {
            for (PoppyDB srv : rsServers) {
                try {
                    if (srv.isRunning()) {
                        srv.shutdown();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    private List<PoppyDB> rsServers;

    /** same observation loop, but against an in-JVM 2-node replica set and with repeated bursts */
    @Test
    void observePoolConvergenceAgainstReplicaSet() throws Exception {
        int p1 = freePort();
        int p2 = freePort();
        List<String> hosts = List.of("127.0.0.1:" + p1, "127.0.0.1:" + p2);
        rsServers = new ArrayList<>();
        rsServers.add(new PoppyDB(p1, "127.0.0.1", 1000, 10));
        rsServers.add(new PoppyDB(p2, "127.0.0.1", 1000, 10));

        for (PoppyDB srv : rsServers) {
            srv.configureReplicaSet("rs_poolrepro", hosts, null, true, null);
        }

        for (PoppyDB srv : rsServers) {
            srv.start();
        }

        long deadline = System.currentTimeMillis() + 20_000;

        while (rsServers.stream().noneMatch(PoppyDB::isPrimary)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("no primary elected");
            }

            Thread.sleep(200);
        }

        PooledDriver drv = makeDriver(hosts.toArray(new String[0]));
        drv.connect();

        while (!drv.isConnected()) {
            Thread.sleep(100);
        }

        Thread.sleep(2000);
        System.out.println("=== after connect: " + drv.getNumConnectionsByHost() + " | " + relevantStats(drv));

        for (int cycle = 1; cycle <= 8; cycle++) {
            List<MongoConnection> held = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                try {
                    held.add(drv.getPrimaryConnection(null));
                } catch (Exception e) {
                    System.out.println("cycle " + cycle + " borrow #" + i + " failed: " + e.getMessage());
                }
            }

            for (MongoConnection c : held) {
                drv.releaseConnection(c);
            }

            // convergence attempt like the real test: all hosts must read exactly 2
            long start = System.currentTimeMillis();
            boolean converged = false;
            Map<String, Integer> last = null;

            while (System.currentTimeMillis() - start < 31_000) {
                Map<String, Integer> now = new TreeMap<>(drv.getNumConnectionsByHost());

                if (!now.equals(last)) {
                    System.out.println("cycle " + cycle + " +" + (System.currentTimeMillis() - start) + "ms conns=" + now
                                       + " | " + relevantStats(drv));
                    last = now;
                }

                if (!now.isEmpty() && now.values().stream().allMatch(v -> v == 2)) {
                    converged = true;
                    break;
                }

                Thread.sleep(100);
            }

            System.out.println("=== cycle " + cycle + (converged ? " CONVERGED after " + (System.currentTimeMillis() - start) + "ms"
                               : " DID NOT CONVERGE: " + drv.getNumConnectionsByHost() + " | " + relevantStats(drv)));

            if (!converged) {
                break;
            }
        }

        drv.close();
    }

    private PooledDriver makeDriver(String... seed) {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(seed);
        drv.setMaxConnections(100);
        drv.setMinConnections(2);
        drv.setConnectionTimeout(2000);
        drv.setMaxConnectionLifetime(1000);
        drv.setMaxConnectionIdleTime(5000);
        drv.setMinConnectionsPerHost(2);
        drv.setMaxWaitTime(2000);
        drv.setHeartbeatFrequency(250);
        return drv;
    }

    @Test
    void observePoolConvergenceAfterBorrowBurst() throws Exception {
        int port = freePort();
        server = new PoppyDB(port, "127.0.0.1", 100, 60);
        server.start();
        Thread.sleep(500);

        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("127.0.0.1:" + port);
        drv.setMaxConnections(100);
        drv.setMinConnections(2);
        drv.setConnectionTimeout(2000);
        drv.setMaxConnectionLifetime(1000);
        drv.setMaxConnectionIdleTime(5000);
        drv.setMinConnectionsPerHost(2);
        drv.setMaxWaitTime(2000);
        drv.setHeartbeatFrequency(250);
        drv.connect();

        while (!drv.isConnected()) {
            Thread.sleep(100);
        }

        Thread.sleep(2000);
        System.out.println("=== after connect: " + drv.getNumConnectionsByHost());

        List<MongoConnection> held = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            held.add(drv.getPrimaryConnection(null));
        }

        for (MongoConnection c : held) {
            drv.releaseConnection(c);
        }

        held.clear();

        for (int i = 0; i < 20; i++) {
            try {
                held.add(drv.getPrimaryConnection(null));
            } catch (Exception e) {
                System.out.println("borrow #" + i + " failed: " + e.getMessage());
            }
        }

        System.out.println("=== borrowed " + held.size() + ": " + drv.getNumConnectionsByHost() + " | " + relevantStats(drv));

        for (MongoConnection c : held) {
            drv.releaseConnection(c);
        }

        System.out.println("=== released all: " + drv.getNumConnectionsByHost() + " | " + relevantStats(drv));

        // convergence window: sample everything until stable or 40s
        Map<String, Integer> lastSnapshot = null;
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < 40_000) {
            Map<String, Integer> now = new TreeMap<>(drv.getNumConnectionsByHost());

            if (!now.equals(lastSnapshot)) {
                System.out.println((System.currentTimeMillis() - start) + "ms conns=" + now + " | " + relevantStats(drv));
                lastSnapshot = now;
            }

            Thread.sleep(100);
        }

        System.out.println("=== final: " + drv.getNumConnectionsByHost() + " | " + relevantStats(drv));
        drv.close();
    }

    private Map<String, Object> relevantStats(PooledDriver drv) {
        Map<String, Object> out = new LinkedHashMap<>();

        for (var e : drv.getConnectionPoolDetails().entrySet()) {
            String k = String.valueOf(e.getKey());

            if (k.contains("pool_size") || k.contains("borrowed_counter") || k.contains("borrowed_map")
                    || k.contains("pending") || k.contains("wait_counter") || k.contains("DRIFT")) {
                out.put(k, e.getValue());
            }
        }

        return out;
    }
}
