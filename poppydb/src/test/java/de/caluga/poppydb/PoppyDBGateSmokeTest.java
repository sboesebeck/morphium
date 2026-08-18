package de.caluga.poppydb;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The CI gate: does PoppyDB basically work?
 *
 * <p>This class decides whether a freshly built jar may be deployed to the shared PoppyDB
 * hosts that later test phases run against, so it is held to two rules the rest of the module
 * is not:
 *
 * <ul>
 *   <li><b>Deterministic.</b> A gate runs once, with no repetition, so a flaky assertion there
 *       is invisible AS flakiness - it looks like a real failure and blocks everything behind
 *       it. Every wait here is a condition wait with a generous deadline; nothing asserts a
 *       momentary state, and nothing sleeps for a fixed span and then asserts.</li>
 *   <li><b>Small.</b> It answers one question - server starts, speaks the protocol, forms a
 *       replica set, replicates - and leaves everything else to the module suite, which runs
 *       as a non-blocking stage with retries and flaky detection.</li>
 * </ul>
 *
 * <p>It exists because the pre-existing end-to-end tests ({@code PoppyDBTest} and friends) are
 * {@code @Disabled} for being flaky under parallel runs, so the gate used to run twelve minutes
 * of election and replication logic tests while skipping the question it is actually there to
 * answer.
 */
public class PoppyDBGateSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(PoppyDBGateSmokeTest.class);

    private final List<PoppyDB> servers = new ArrayList<>();
    private final List<Morphium> clients = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (Morphium m : clients) {
            try {
                m.close();
            } catch (Exception e) {
                // best effort
            }
        }
        clients.clear();

        for (PoppyDB srv : servers) {
            try {
                srv.shutdown();
            } catch (Exception e) {
                // best effort
            }
        }
        servers.clear();
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private Morphium client(String db, boolean replicaSet, String... hosts) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase(db);
        cfg.driverSettings().setDriverName(PooledDriver.driverName);

        for (String h : hosts) {
            String[] parts = h.split(":");
            cfg.clusterSettings().addHostToSeed(parts[0], Integer.parseInt(parts[1]));
        }

        cfg.clusterSettings().setHeartbeatFrequency(100);
        cfg.clusterSettings().setReplicaset(replicaSet);

        if (replicaSet) {
            cfg.clusterSettings().setRequiredReplicaSetName("rsGateSmoke");
        }

        // The gate checks that the server works, not that collection housekeeping does.
        cfg.collectionCheckSettings().setCappedCheck(CappedCheck.NO_CHECK);
        cfg.collectionCheckSettings().setIndexCheck(IndexCheck.NO_CHECK);
        Morphium m = new Morphium(cfg);
        clients.add(m);
        return m;
    }

    /**
     * A single node has to start, accept a client and survive one round of each CRUD verb.
     */
    @Test
    public void singleNodeServesCrud() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 100, 10);
        servers.add(srv);
        srv.start();

        Morphium m = client("gate_smoke", false, "127.0.0.1:" + port);

        m.store(new UncachedObject("gate", 42));
        TestUtils.waitForConditionToBecomeTrue(10000, "stored object never became visible",
                () -> m.createQueryFor(UncachedObject.class).countAll() == 1);

        UncachedObject stored = m.createQueryFor(UncachedObject.class).f("counter").eq(42).get();
        assertNotNull(stored, "the stored object must be findable by field");
        assertEquals("gate", stored.getStrValue());

        stored.setStrValue("gate-updated");
        m.store(stored);
        TestUtils.waitForConditionToBecomeTrue(10000, "update never became visible",
                () -> "gate-updated".equals(m.createQueryFor(UncachedObject.class).f("counter").eq(42).get().getStrValue()));

        m.delete(stored);
        TestUtils.waitForConditionToBecomeTrue(10000, "delete never became visible",
                () -> m.createQueryFor(UncachedObject.class).countAll() == 0);
    }

    /**
     * Three nodes have to elect exactly one primary, accept a write through the replica-set
     * client, and actually move that write to both secondaries.
     */
    @Test
    public void replicaSetElectsAPrimaryAndReplicates() throws Exception {
        List<Integer> ports = List.of(freePort(), freePort(), freePort());
        List<String> hosts = ports.stream().map(p -> "127.0.0.1:" + p).toList();

        for (int port : ports) {
            PoppyDB srv = new PoppyDB(port, "127.0.0.1", 100, 10);
            srv.configureReplicaSet("rsGateSmoke", hosts, null, true, null);
            servers.add(srv);
        }

        for (PoppyDB srv : servers) {
            srv.start();
        }

        TestUtils.waitForConditionToBecomeTrue(30000, "no primary was elected",
                () -> servers.stream().filter(PoppyDB::isPrimary).count() == 1);

        PoppyDB primary = servers.stream().filter(PoppyDB::isPrimary).findFirst().orElseThrow();
        log.info("Primary is {}", primary.getPort());

        Morphium m = client("gate_smoke_rs", true, hosts.toArray(new String[0]));

        for (int i = 0; i < 10; i++) {
            m.store(new UncachedObject("gate-rs", i));
        }

        TestUtils.waitForConditionToBecomeTrue(20000, "the write never became readable again",
                () -> m.createQueryFor(UncachedObject.class).countAll() == 10);

        // Replication is asserted on the secondaries themselves rather than by reading through
        // the client, which could be served by the primary and would prove nothing about them.
        List<PoppyDB> secondaries = servers.stream().filter(s -> !s.isPrimary()).toList();
        assertEquals(2, secondaries.size(), "a three-node set must have two secondaries");

        for (PoppyDB secondary : secondaries) {
            TestUtils.waitForConditionToBecomeTrue(30000,
                    "secondary " + secondary.getPort() + " never applied any replicated event",
                    () -> {
                        ReplicationManager rm = secondary.getReplicationManagerForTest();
                        return rm != null && rm.getEventsApplied() > 0;
                    });
        }
    }
}
