package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("failover")
@Tag("morphiumserver")
public class FailoverTest {
    private static final Logger log = LoggerFactory.getLogger(FailoverTest.class);
    private static int lastPort = 17017;

    private synchronized int nextPort() {
        return lastPort++;
    }

    private void startServer(MorphiumServer srv, int port) throws Exception {
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private void waitForPrimary(String context, MorphiumServer... servers) throws Exception {
        TestUtils.waitForConditionToBecomeTrue(10_000, "No primary elected for " + context, () -> {
            for (MorphiumServer s : servers) {
                try {
                    if (s.isPrimary()) return true;
                } catch (Exception e) {
                    // Server might be terminated, skip it
                }
            }
            return false;
        });
    }

    private MorphiumServer findPrimary(MorphiumServer... servers) {
        for (MorphiumServer s : servers) {
            try {
                if (s.isPrimary()) return s;
            } catch (Exception e) {
                // Server might be terminated, skip it
            }
        }
        return null;
    }

    @Test
    public void testFailoverWithPooledDriver() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();

        MorphiumServer s1 = new MorphiumServer(port1, "localhost", 100, 10);
        MorphiumServer s2 = new MorphiumServer(port2, "localhost", 100, 10);
        MorphiumServer s3 = new MorphiumServer(port3, "localhost", 100, 10);

        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prios = Map.of("localhost:" + port1, 300, "localhost:" + port2, 200, "localhost:" + port3, 100);

        s1.configureReplicaSet("rsFailover", hosts, prios);
        s2.configureReplicaSet("rsFailover", hosts, prios);
        s3.configureReplicaSet("rsFailover", hosts, prios);

        startServer(s1, port1);
        startServer(s2, port2);
        startServer(s3, port3);

        // Start replication on all nodes
        s1.startReplicaReplication();
        s2.startReplicaReplication();
        s3.startReplicaReplication();

        // Wait for election using polling instead of fixed sleep
        waitForPrimary("initial election", s1, s2, s3);
        assertTrue(s1.isPrimary(), "s1 should be primary due to highest priority");

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed(hosts);
        cfg.setDatabase("failover_test");
        cfg.setDriverName(PooledDriver.driverName);
        cfg.clusterSettings().setReplicaSetMonitoringTimeout(1000);
        cfg.setHeartbeatFrequency(500);
        cfg.setMaxConnections(10);
        cfg.setMinConnections(1);

        Morphium morphium = new Morphium(cfg);
        try {
            // Initial write
            UncachedObject uc1 = new UncachedObject("value1", 1);
            morphium.store(uc1);
            log.info("Initial write successful");

            // Shut down primary
            log.info("Terminating primary (s1 on port {})", port1);
            s1.terminate();

            // Wait for failover - only check s2 and s3 since s1 is terminated
            log.info("Waiting for failover...");
            waitForPrimary("failover after s1 termination", s2, s3);
            MorphiumServer newPrimary = findPrimary(s2, s3);
            log.info("Failover successful, new primary is {}", newPrimary == s2 ? "s2" : "s3");

            // Perform write after failover
            UncachedObject uc2 = new UncachedObject("value2", 2);
            morphium.store(uc2);
            log.info("Write after failover successful");

            // Verify data consistency
            List<UncachedObject> all = morphium.createQueryFor(UncachedObject.class).asList();
            assertEquals(2, all.size());
            
            // Bring s1 back as secondary
            log.info("Bringing s1 back...");
            s1 = new MorphiumServer(port1, "localhost", 100, 10);
            s1.configureReplicaSet("rsFailover", hosts, prios);
            startServer(s1, port1);
            s1.startReplicaReplication();

            // Wait for s1 to join the cluster - it should initially be secondary
            // because another primary already exists
            Thread.sleep(1000);
            log.info("After s1 rejoins: s1.isPrimary={}, s2.isPrimary={}, s3.isPrimary={}",
                     s1.isPrimary(), s2.isPrimary(), s3.isPrimary());

            // Verify cluster has exactly one primary
            int primaryCount = (s1.isPrimary() ? 1 : 0) + (s2.isPrimary() ? 1 : 0) + (s3.isPrimary() ? 1 : 0);
            assertEquals(1, primaryCount, "Cluster should have exactly one primary");

            // Perform another write - should work regardless of which node is primary
            UncachedObject uc3 = new UncachedObject("value3", 3);
            morphium.store(uc3);
            log.info("Write after s1 rejoin successful");

            assertEquals(3, morphium.createQueryFor(UncachedObject.class).countAll());

        } finally {
            morphium.close();
            s1.terminate();
            s2.terminate();
            s3.terminate();
        }
    }

    @Test
    public void testFailoverWithSingleConnectDriver() throws Exception {
        int port1 = nextPort() + 100; // avoid port conflicts
        int port2 = nextPort() + 100;
        int port3 = nextPort() + 100;

        MorphiumServer s1 = new MorphiumServer(port1, "localhost", 100, 10);
        MorphiumServer s2 = new MorphiumServer(port2, "localhost", 100, 10);
        MorphiumServer s3 = new MorphiumServer(port3, "localhost", 100, 10);

        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prios = Map.of("localhost:" + port1, 300, "localhost:" + port2, 200, "localhost:" + port3, 100);

        s1.configureReplicaSet("rsFailoverSingle", hosts, prios);
        s2.configureReplicaSet("rsFailoverSingle", hosts, prios);
        s3.configureReplicaSet("rsFailoverSingle", hosts, prios);

        startServer(s1, port1);
        startServer(s2, port2);
        startServer(s3, port3);

        s1.startReplicaReplication();
        s2.startReplicaReplication();
        s3.startReplicaReplication();

        // Wait for election using polling instead of fixed sleep
        waitForPrimary("initial election (SingleConnect)", s1, s2, s3);

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed(hosts);
        cfg.setDatabase("failover_test_single");
        cfg.setDriverName(SingleMongoConnectDriver.driverName);
        cfg.clusterSettings().setReplicaSetMonitoringTimeout(1000);
        cfg.setHeartbeatFrequency(100);

        Morphium morphium = new Morphium(cfg);
        try {
            morphium.store(new UncachedObject("v1", 1));

            log.info("Terminating primary (s1)");
            s1.terminate();

            // SingleConnectDriver might need more time or retries
            log.info("Waiting for failover and write to succeed...");
            TestUtils.waitForConditionToBecomeTrue(30000, "Failover with SingleConnectDriver failed", () -> {
                // Only check s2 and s3 since s1 is terminated
                log.info("Primary status: s2={}, s3={}", s2.isPrimary(), s3.isPrimary());
                try {
                    morphium.store(new UncachedObject("v2", 2));
                    return true;
                } catch (Exception e) {
                    log.info("Write failed, still waiting for failover: {}", e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                }
            });
            log.info("Write after failover successful, new primary is {}", s2.isPrimary() ? "s2" : "s3");

            assertEquals(2, morphium.createQueryFor(UncachedObject.class).countAll());
        } finally {
            morphium.close();
            s1.terminate();
            s2.terminate();
            s3.terminate();
        }
    }
}
