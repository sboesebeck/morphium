package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * E2E regression test for the empty-node-restart cluster-wide data-loss bug
 * (2026-08-14-poppydb-empty-node-wipe) - pins down the exact real-world repro forever, at the
 * full multi-node in-process replica-set level (election + replication wired together, unlike
 * {@link ReplicationFailClosedTest} which exercises the replication-only D2 guard in isolation).
 *
 * <p><b>The bug, as it happened in production:</b> a 3-node replica set (distinct priorities),
 * fully replicated. The highest-priority node was killed and restarted EMPTY (fresh process, no
 * on-disk/in-memory state, same address). Before the fixes on this branch:
 * <ol>
 *   <li>the empty, freshly-restarted node won the election on priority alone, despite having
 *       zero data (its {@code lastLogIndex} started at 0, but nothing stopped it campaigning or
 *       being granted votes purely on priority/term);</li>
 *   <li>the data-bearing followers, reconnecting to what was now "the primary", found their
 *       replicated namespace set didn't match the empty leader's and fell back to a full
 *       resync - which meant dropping their own (real, correct) databases first: "Falling back
 *       to full sync: replicated namespace sets differ (primary: {}, local: {...})".</li>
 * </ol>
 * Net effect: an operational restart of a single, already-caught-up node wiped the entire
 * cluster's data.
 *
 * <p><b>The fixes under test</b> (all already on this branch): vote safety + candidacy restraint
 * so an empty node cannot win an election while data-bearing peers exist (commits
 * d6735e0ee/c8a5cb669, 2ee1848bf), freshly-synced nodes reporting their true replication position
 * to the election instead of a stale 0 (47877ad18), and a fail-closed guard on the replication
 * side that refuses a destructive resync from a primary whose sequence has regressed relative to
 * local state (49633aba6). This test does not target any one of those commits individually - it
 * pins the OUTCOME: no matter which layer is doing the protecting, the cluster must survive this
 * kill chain with zero data loss.
 *
 * <p><b>Design notes:</b>
 * <ul>
 *   <li>"Restart empty" is reproduced literally: the node is hard {@link PoppyDB#shutdown()}, and
 *       a brand-new {@code PoppyDB} instance - fresh in-memory driver, fresh election state - is
 *       started on the exact same port, exactly like a process manager restarting a crashed
 *       server (modeled on {@link ReplicationFailClosedTest}'s "kill primary, start fresh empty
 *       PoppyDB on the same port" trick, here at the full RS/election level instead of a
 *       manually-wired {@link ReplicationManager}).</li>
 *   <li>Every count is read via {@link PoppyDB#getDriver()} directly against each node's own
 *       in-memory driver, never over the wire - secondaries reject unqualified wire reads by
 *       design (see {@code b15c28704}), so a wire-level count would silently only ever prove the
 *       primary's view, not each node's own local state, which is exactly what a wipe would
 *       corrupt.</li>
 *   <li>{@link #watchConvergence} is an ACTIVE watch, not a single condition-poll: on every tick
 *       (150ms) it re-asserts that the still-data-bearing nodes have not lost anything, and that
 *       the restarted node - if it currently claims leadership - already has the full data set.
 *       This catches a transient wipe-then-recover as reliably as a permanent one, and catches a
 *       "won leadership while still empty" violation the instant it happens rather than only if
 *       it happens to still be true whenever a single poll happens to sample it.</li>
 *   <li>Priority takeover timers are shortened ({@link #fastTakeoverConfig()}) purely to keep the
 *       "restarted highest-priority node may reclaim leadership, but only once synced" leg of
 *       Test A actually exercised within the test's timeout, rather than leaving it as a
 *       might-or-might-not-happen possibility under the 30s default stability window.</li>
 * </ul>
 */
@Tag("server")
public class EmptyNodeRestartWipeTest {

    private static final Logger log = LoggerFactory.getLogger(EmptyNodeRestartWipeTest.class);

    private static final String DB = "emptynodewipe";
    private static final String COLL = "objs";
    private static final int DOCS = 100;

    /** Started nodes, shut down in reverse start order on teardown. */
    private final List<PoppyDB> nodes = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    // ---- RS bootstrap helpers (pattern of UserFailoverTest / StepdownReplicationTest) -------

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        nodes.add(srv);
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

    private void waitForPrimary(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!node.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(node.isPrimary(), "node must become primary");
    }

    /** Poll a condition with generous timeout - replication/election is asynchronous, never fixed-sleep. */
    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(100);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    /**
     * A fresh {@link ElectionConfig} instance per call (never share one instance across nodes -
     * {@code PoppyDB#configureReplicaSet} mutates its config's priority in place, which would
     * silently cross-contaminate every node handed the same object) with shortened priority
     * takeover timers, so a caught-up higher-priority node reclaiming leadership is something
     * this test can actually observe within its timeout instead of only maybe happening within
     * the 30s production default.
     */
    private ElectionConfig fastTakeoverConfig() {
        return new ElectionConfig()
            .setPriorityTakeoverMinStabilityMs(3000)
            .setPriorityTakeoverCheckIntervalMs(1000)
            .setPriorityTakeoverStepDownSecs(3);
    }

    // ---- data helpers ------------------------------------------------------------------------

    /** Reads the doc count directly off the node's OWN local driver - never over the wire. */
    private long countOn(PoppyDB node) {
        return node.getDriver().count(DB, COLL, Doc.of(), null, null);
    }

    private Morphium writerFor(int port, String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(db);
        cfg.connectionSettings().setMaxConnections(10);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        return new Morphium(cfg);
    }

    private void writeDocs(Morphium writer, int count, String prefix) {
        List<UncachedObject> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(new UncachedObject(prefix + "-" + i, i));
        }
        writer.storeList(batch, COLL);
    }

    /**
     * Actively watches convergence after an empty-node restart, for up to {@code timeoutMs}:
     * <ul>
     *   <li>on EVERY tick, every node in {@code mustNeverLoseData} must still report exactly
     *       {@code expectedCount} - a real wipe, even a transient one, fails the test the moment
     *       it is observed, not just if it happens to still be visible at the end;</li>
     *   <li>on EVERY tick, if {@code restarted} currently claims leadership
     *       ({@link PoppyDB#isPrimary()}), it must already report {@code expectedCount} locally -
     *       leadership while still behind/empty is exactly the bug this test pins;</li>
     *   <li>returns as soon as {@code restarted} itself reaches {@code expectedCount} (legitimate
     *       convergence via initial sync); fails with a descriptive message if that never happens
     *       within {@code timeoutMs}.</li>
     * </ul>
     */
    private void watchConvergence(PoppyDB restarted, List<PoppyDB> mustNeverLoseData,
            long expectedCount, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            for (PoppyDB n : mustNeverLoseData) {
                long c = countOn(n);
                assertEquals(expectedCount, c,
                    "a data-bearing node must never lose data while the empty node restarts/resyncs "
                        + "(got " + c + ", want " + expectedCount + ")");
            }
            if (restarted.isPrimary()) {
                long c = countOn(restarted);
                assertEquals(expectedCount, c,
                    "the restarted node must never hold/claim leadership before its own data has "
                        + "fully caught up via legitimate initial sync (local count=" + c
                        + ", want " + expectedCount + ")");
            }
            long restartedCount = countOn(restarted);
            if (restartedCount == expectedCount) {
                return; // converged: restarted node has legitimately caught up
            }
            if (System.currentTimeMillis() > deadline) {
                fail("restarted node never reached the full document count via initial sync "
                    + "(got " + restartedCount + ", want " + expectedCount + ") within " + timeoutMs + "ms");
            }
            Thread.sleep(150);
        }
    }

    // ---- Test A: restart the HIGHEST-priority node empty ------------------------------------

    @Test
    public void restartingHighestPriorityNodeEmptyMustNotWipeTheCluster() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 90,
                "localhost:" + port3, 80);
        node1.configureReplicaSet("rsEmptyWipeA", hosts, prio, true, fastTakeoverConfig());
        node2.configureReplicaSet("rsEmptyWipeA", hosts, prio, true, fastTakeoverConfig());
        node3.configureReplicaSet("rsEmptyWipeA", hosts, prio, true, fastTakeoverConfig());

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1); // priority 100 wins the initial election deterministically

        Morphium writer = writerFor(port1, DB);
        try {
            writeDocs(writer, DOCS, "pre");
        } finally {
            writer.close();
        }

        // Replicated everywhere BEFORE we touch anything - isolates the restart-empty scenario
        // below from an ordinary steady-state replication bug.
        assertTrue(poll(30_000, () -> countOn(node1) == DOCS && countOn(node2) == DOCS && countOn(node3) == DOCS),
                "all " + DOCS + " docs must replicate to every node before the kill (node1=" + countOn(node1)
                        + ", node2=" + countOn(node2) + ", node3=" + countOn(node3) + ")");

        log.info("Test A: pre-kill state converged, {} docs on all 3 nodes; killing node1 (highest priority)", DOCS);

        // ---- the exact repro: hard-kill the highest-priority node, restart it EMPTY on the same port ----
        node1.shutdown();
        nodes.remove(node1);

        PoppyDB restarted = new PoppyDB(port1, "localhost", 20, 5);
        restarted.configureReplicaSet("rsEmptyWipeA", hosts, prio, true, fastTakeoverConfig());
        startServer(restarted, port1);

        // The heart of the regression: while the cluster converges, node2/node3's real data must
        // never be wiped, and the restarted node may only ever claim leadership once it has
        // genuinely caught up via initial sync - never while it is still empty/behind.
        watchConvergence(restarted, List.of(node2, node3), DOCS, 90_000);

        // Final full-cluster assertion, each read directly off the node's own local driver state.
        assertEquals(DOCS, countOn(restarted), "restarted node must have fully synced");
        assertEquals(DOCS, countOn(node2), "node2 must still have all data after convergence");
        assertEquals(DOCS, countOn(node3), "node3 must still have all data after convergence");

        // Best-effort confirmation that the restarted node reached that count via a legitimate
        // initial sync (not e.g. having become primary itself and thus having no ReplicationManager
        // to ask - that path is already independently proven correct by watchConvergence above,
        // since it could only have claimed leadership once already fully synced).
        ReplicationManager restartedRm = restarted.getReplicationManagerForTest();
        if (restartedRm != null) {
            assertTrue(restartedRm.isInitialSyncComplete(),
                    "the restarted node's ReplicationManager must report a COMPLETED initial sync");
        }

        log.info("Test A converged: restarted node reached {} docs, cluster primary is now {}",
                countOn(restarted), node2.isPrimary() ? "node2" : (node3.isPrimary() ? "node3" : "restarted"));
    }

    // ---- Test B: restart the LOWEST-priority node empty --------------------------------------

    @Test
    public void restartingLowestPriorityNodeEmptyMustNotWipeTheCluster() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 90,
                "localhost:" + port3, 80);
        node1.configureReplicaSet("rsEmptyWipeB", hosts, prio, true, fastTakeoverConfig());
        node2.configureReplicaSet("rsEmptyWipeB", hosts, prio, true, fastTakeoverConfig());
        node3.configureReplicaSet("rsEmptyWipeB", hosts, prio, true, fastTakeoverConfig());

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1); // priority 100 wins the initial election deterministically

        Morphium writer = writerFor(port1, DB);
        try {
            writeDocs(writer, DOCS, "pre");
        } finally {
            writer.close();
        }

        assertTrue(poll(30_000, () -> countOn(node1) == DOCS && countOn(node2) == DOCS && countOn(node3) == DOCS),
                "all " + DOCS + " docs must replicate to every node before the kill (node1=" + countOn(node1)
                        + ", node2=" + countOn(node2) + ", node3=" + countOn(node3) + ")");

        log.info("Test B: pre-kill state converged, {} docs on all 3 nodes; killing node3 (lowest priority)", DOCS);

        // ---- restart the LOWEST-priority node empty: node1 stays primary throughout, no ----
        // ---- re-election is even needed - this isolates the wipe-on-resync half of the bug ----
        // ---- from the vote/candidacy half that Test A exercises. ----
        node3.shutdown();
        nodes.remove(node3);

        PoppyDB restarted = new PoppyDB(port3, "localhost", 20, 5);
        restarted.configureReplicaSet("rsEmptyWipeB", hosts, prio, true, fastTakeoverConfig());
        startServer(restarted, port3);

        // node1 (still primary, highest priority, never touched) and node2 must never lose data;
        // the restarted lowest-priority node must never claim leadership before catching up
        // (trivially true here since node1 never yields it, but the same watch applies uniformly).
        watchConvergence(restarted, List.of(node1, node2), DOCS, 90_000);

        assertEquals(DOCS, countOn(restarted), "restarted node must have fully synced");
        assertEquals(DOCS, countOn(node1), "node1 (primary throughout) must still have all data");
        assertEquals(DOCS, countOn(node2), "node2 must still have all data after convergence");
        assertTrue(node1.isPrimary(), "node1 must have remained primary the whole time - no failover was needed");

        ReplicationManager restartedRm = restarted.getReplicationManagerForTest();
        if (restartedRm != null) {
            assertTrue(restartedRm.isInitialSyncComplete(),
                    "the restarted node's ReplicationManager must report a COMPLETED initial sync");
        }

        log.info("Test B converged: restarted node reached {} docs, node1 remained primary throughout", countOn(restarted));
    }
}
