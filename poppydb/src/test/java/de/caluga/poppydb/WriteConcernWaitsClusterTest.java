package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * #529 on a real three-node set: a write-concern wait must not outlive (or block) the leadership
 * it belongs to.
 *
 * <p>The prod incident this comes from: after a leader change back to the same node, every w&gt;=2
 * write on it timed out for minutes, because the waits of the previous leadership still held the
 * fixed command-pool threads and the writes of the new leadership queued behind them. Here the
 * same shape is driven end to end - steady w=2 writes, the leader is forced away and back, and
 * the first w=2 write on the returned primary must be acknowledged within 5 s of its takeover,
 * with {@code serverStatus.writeConcernWaits.waiting} back at 0.
 *
 * <p>Second case: on an idle set nothing advances, so nothing was reported - and a secondary that
 * does not report is a dead secondary as far as write concerns are concerned. The heartbeat
 * report keeps the primary's view of its secondaries live without any writes.
 */
@Tag("server")
public class WriteConcernWaitsClusterTest {

    private static final String DB = "wcwaits";
    private static final String COLL = "docs";
    private static final String RS_NAME = "rsWriteConcernWaits";

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final List<PoppyDB> nodes = new ArrayList<>();
    private PoppyDB node1;
    private PoppyDB node2;
    private PoppyDB node3;
    private int port1;
    private int port2;
    private int port3;

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

    // ---- cluster bootstrap (pattern of StepdownReplicationTest) ----

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

    /**
     * Three election-mode nodes with strictly ordered priorities (node1 &gt; node2 &gt; node3) so
     * the initial election and the failback are decided by priority, not by timing luck. Test-speed
     * election timers; the priority takeover is on with a short stability window, so a lower-
     * priority leader yields to node1 as soon as node1 is a fresh, caught-up follower again.
     */
    private void startThreeNodeSet() throws Exception {
        port1 = nextPort();
        port2 = nextPort();
        port3 = nextPort();
        node1 = new PoppyDB(port1, "localhost", 20, 5);
        node2 = new PoppyDB(port2, "localhost", 20, 5);
        node3 = new PoppyDB(port3, "localhost", 20, 5);

        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of("localhost:" + port1, 100,
                                           "localhost:" + port2, 50,
                                           "localhost:" + port3, 10);

        node1.configureReplicaSet(RS_NAME, hosts, prio, true, electionConfig());
        node2.configureReplicaSet(RS_NAME, hosts, prio, true, electionConfig());
        node3.configureReplicaSet(RS_NAME, hosts, prio, true, electionConfig());

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
    }

    private static ElectionConfig electionConfig() {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(400)
                .setElectionTimeoutMaxMs(800)
                .setHeartbeatIntervalMs(150)
                .setLeaderLeaseTimeoutMs(4000)
                .setPriorityTakeoverEnabled(true)
                .setPriorityTakeoverCheckIntervalMs(300)
                .setPriorityTakeoverMinStabilityMs(1000)
                .setPriorityTakeoverStepDownSecs(1);
    }

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(50);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    private void awaitPrimary(PoppyDB node, String what) throws Exception {
        assertTrue(poll(40_000, node::isPrimary), what + ": node must become primary");
    }

    /**
     * A w=2 write sent before any secondary has reported is answered "no secondaries" after
     * 100 ms - correctly, but not what these tests are about. On a loaded host the secondaries
     * need longer for their initial sync than the test needs to send its first write.
     */
    private void awaitSecondariesLive(PoppyDB primary) throws Exception {
        assertTrue(poll(40_000, () -> {
            ReplicationCoordinator coordinator = primary.getReplicationCoordinator();
            return coordinator != null && coordinator.getActiveSecondaryCount() == 2;
        }), "both secondaries must have reported to the primary");
    }

    private PoppyDB currentPrimaryOtherThan(PoppyDB notThis) {
        for (PoppyDB node : List.of(node1, node2, node3)) {
            if (node != notThis && node.isPrimary()) {
                return node;
            }
        }
        return null;
    }

    // ---- wire helpers ----

    private Map<String, Object> command(int port, Map<String, Object> cmd) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port), 2000);
            sock.setSoTimeout(20_000);
            OpMsg msg = new OpMsg();
            msg.setMessageId(MSG_ID.incrementAndGet());
            msg.setFlags(0);
            msg.setFirstDoc(cmd);
            sock.getOutputStream().write(msg.bytes());
            sock.getOutputStream().flush();
            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
            return reply.getFirstDoc();
        }
    }

    private Map<String, Object> insert(int port, String id, int w, int wtimeout) throws Exception {
        return command(port, Doc.of(
                "insert", COLL,
                "documents", List.of(Doc.of("_id", id, "v", id)),
                "$db", DB,
                "writeConcern", Doc.of("w", w, "wtimeout", wtimeout)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> writeConcernWaits(int port) throws Exception {
        Map<String, Object> status = command(port, Doc.of("serverStatus", 1, "$db", "admin"));
        assertEquals(1.0, ((Number) status.get("ok")).doubleValue(), "serverStatus must answer: " + status);
        Object section = status.get("writeConcernWaits");
        assertNotNull(section, "serverStatus must carry writeConcernWaits: " + status.keySet());
        return (Map<String, Object>) section;
    }

    // ---- the incident's shape ----

    private static int code(Map<String, Object> reply) {
        @SuppressWarnings("unchecked")
        Map<String, Object> wce = (Map<String, Object>) reply.get("writeConcernError");
        return wce == null ? 0 : ((Number) wce.get("code")).intValue();
    }

    /**
     * Writes are waiting for their acknowledgement when the leader changes - the secondaries keep
     * replicating but their progress reports are held back. Those waits must be answered at once
     * as PrimarySteppedDown, and after the failback the returned primary must acknowledge w=2
     * writes again right away instead of serving the dead waits of its previous leadership.
     */
    @Test
    @Timeout(240)
    public void w2WritesAreAcknowledgedAgainRightAfterTheNodeTookTheLeadBack() throws Exception {
        startThreeNodeSet();
        awaitPrimary(node1, "initial election");
        awaitSecondariesLive(node1);

        // Steady state: w=2 writes are acknowledged by the secondaries.
        for (int i = 0; i < 5; i++) {
            Map<String, Object> reply = insert(port1, "steady-" + i, 2, 5_000);
            assertEquals(1.0, ((Number) reply.get("ok")).doubleValue(), "steady w=2 insert: " + reply);
            assertNull(reply.get("writeConcernError"), "a healthy 3-node set can satisfy w=2: " + reply);
        }

        // Hold the acknowledgements back: both secondaries keep replicating, but neither reports.
        List<ReplicationManager> held = new ArrayList<>();
        for (PoppyDB secondary : List.of(node2, node3)) {
            ReplicationManager rm = secondary.getReplicationManagerForTest();
            assertNotNull(rm, "a secondary has a replication manager");
            rm.holdProgressReportsForTest(true);
            held.add(rm);
        }
        Thread.sleep(300); // a report already on the wire must not acknowledge the writes below

        // Several clients, each with a w=2 write and a long wtimeout - in flight at the step-down.
        int inFlight = 4;
        ExecutorService clients = Executors.newFixedThreadPool(inFlight);
        List<Future<Map<String, Object>>> pending = new ArrayList<>();
        try {
            for (int i = 0; i < inFlight; i++) {
                String id = "in-flight-" + i;
                pending.add(clients.submit(() -> insert(port1, id, 2, 60_000)));
            }
            ReplicationCoordinator leadership1 = node1.getReplicationCoordinator();
            assertNotNull(leadership1);
            assertTrue(poll(10_000, () -> ((Number) leadership1.getWaitStats().get("waiting")).intValue() == inFlight),
                    "all " + inFlight + " writes must be waiting: " + leadership1.getWaitStats());

            // Force the leader change away from node1 ...
            long stepDownAtMs = System.currentTimeMillis();
            assertTrue(node1.getElectionManager().stepDown(1, 0, true), "node1 must step down");

            // (a) the waits are answered at once as PrimarySteppedDown, ok:1 - not after 60 s.
            for (Future<Map<String, Object>> future : pending) {
                Map<String, Object> reply = future.get(5, TimeUnit.SECONDS);
                assertEquals(1.0, ((Number) reply.get("ok")).doubleValue(), "the write was executed: " + reply);
                assertEquals(189, code(reply), "a wait released by the step-down is PrimarySteppedDown: " + reply);
            }
            assertTrue(System.currentTimeMillis() - stepDownAtMs < 5_000,
                    "the in-flight writes must be answered right after the step-down");
        } finally {
            clients.shutdownNow();
        }

        for (ReplicationManager rm : held) {
            rm.holdProgressReportsForTest(false);
        }
        assertTrue(poll(40_000, () -> currentPrimaryOtherThan(node1) != null),
                "another node must take over after node1's step-down");
        PoppyDB otherPrimary = currentPrimaryOtherThan(node1);
        assertNotNull(otherPrimary);

        // ... and back: the lower-priority leader yields to node1 (priority takeover), and if the
        // takeover does not fire, the same effect is forced by stepping the incumbent down.
        if (!poll(15_000, node1::isPrimary)) {
            otherPrimary.getElectionManager().stepDown(1, 0, true);
        }
        assertTrue(poll(60_000, node1::isPrimary), "node1 must take the lead back");
        long takeoverAtMs = System.currentTimeMillis();

        // (b) the first w=2 write after the failback is acknowledged within 5 s of the takeover.
        Map<String, Object> acknowledged = null;
        Map<String, Object> lastReply = null;
        while (System.currentTimeMillis() - takeoverAtMs < 5_000) {
            Map<String, Object> reply;
            try {
                reply = insert(port1, "after-failback-" + System.currentTimeMillis(), 2, 5_000);
            } catch (Exception e) {
                // The client's connection may still meet a node in transition; the shape under
                // test is the write concern, so a failed connect is simply retried.
                Thread.sleep(50);
                continue;
            }

            lastReply = reply;
            if (Double.valueOf(1.0).equals(reply.get("ok")) && reply.get("writeConcernError") == null) {
                acknowledged = reply;
                break;
            }
            Thread.sleep(50);
        }

        long afterTakeoverMs = System.currentTimeMillis() - takeoverAtMs;
        assertNotNull(acknowledged, "a w=2 write on the returned primary must be acknowledged within 5 s "
                + "of its takeover, took " + afterTakeoverMs + " ms; last reply: " + lastReply);

        // (c) nothing is left waiting, and the counters of both leaderships are there.
        assertTrue(poll(5_000, () -> {
            try {
                return ((Number) writeConcernWaits(port1).get("waiting")).intValue() == 0;
            } catch (Exception e) {
                return false;
            }
        }), "serverStatus.writeConcernWaits.waiting must return to 0");

        Map<String, Object> primarySection = writeConcernWaits(port1);
        for (String field : List.of("waiting", "oldestWaitMs", "acknowledged", "timedOut",
                "noSecondaries", "steppedDown")) {
            assertTrue(primarySection.containsKey(field), "writeConcernWaits." + field + " is missing: "
                    + primarySection);
        }
        assertTrue(((Number) primarySection.get("steppedDown")).longValue() >= inFlight,
                "the waits released by the step-down must be counted: " + primarySection);
        // 5 steady writes under the first coordinator + 1 under the one created on the failback:
        // the counters live outside the coordinator and survive its replacement.
        assertTrue(((Number) primarySection.get("acknowledged")).longValue() >= 6,
                "the acknowledged waits of both leaderships must be counted: " + primarySection);

        Map<String, Object> secondarySection = writeConcernWaits(port2);
        assertEquals(0, ((Number) secondarySection.get("waiting")).intValue(),
                "a node without a coordinator has nothing waiting: " + secondarySection);
    }

    // ---- liveness without progress ----

    /**
     * On an idle set no sequence advances, so {@code reportProgressToPrimary} had nothing to
     * report - after 30 s every secondary read as dead and {@code activeSecondaries} as 0, which
     * is exactly the state in which a w&gt;=2 write can never be acknowledged. The secondary now
     * reports a heartbeat every 5 s, refreshed by the same command the acks use.
     */
    @Test
    @Timeout(180)
    public void anIdleSetKeepsItsSecondariesLive() throws Exception {
        startThreeNodeSet();
        awaitPrimary(node1, "initial election");
        awaitSecondariesLive(node1);

        // One write with w=2, then silence - nothing but the heartbeat report keeps this alive.
        Map<String, Object> reply = insert(port1, "only-write", 2, 10_000);
        assertNull(reply.get("writeConcernError"), "the set must satisfy w=2 before going idle: " + reply);

        ReplicationCoordinator coordinator = node1.getReplicationCoordinator();
        assertNotNull(coordinator, "the primary must have a replication coordinator");
        assertTrue(poll(30_000, () -> coordinator.getActiveSecondaryCount() == 2),
                "both secondaries must be live before the idle window");

        Thread.sleep(12_000);

        assertEquals(2, coordinator.getActiveSecondaryCount(),
                "the secondaries must still count as live after 12 s without a single write");

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> secondaries =
                (Map<String, Map<String, Object>>) coordinator.getStats().get("secondaries");
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Map<String, Object>> entry : secondaries.entrySet()) {
            long lastHeartbeat = ((Number) entry.getValue().get("lastHeartbeat")).longValue();
            long ageMs = now - lastHeartbeat;
            assertTrue(ageMs < 7_000, "secondary " + entry.getKey() + " last reported " + ageMs
                    + " ms ago - an idle set must keep reporting every 5 s");
        }
    }
}
