package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * Consistency shortcut on leader change (follow-up task 2): when a follower is re-pointed at a
 * new primary after a failover, its fresh {@link ReplicationManager} must NOT wipe and re-copy
 * everything if the local data already matches the new primary byte-for-byte (the common case -
 * both survivors replicated from the same dead leader). The dbHash comparison decides.
 *
 * Two sides of the coin:
 * <ul>
 *   <li>{@link #shortcutTakenWhenDataIdentical()} - identical survivors: the re-targeted
 *       follower converges via the shortcut ({@code wasLastSyncShortcut()} true), data and
 *       users intact.</li>
 *   <li>{@link #fallbackOnDivergence()} - a follower whose local state was made to diverge
 *       (test-only backdoor write into its InMemoryDriver) must take today's full clear +
 *       snapshot path ({@code wasLastSyncShortcut()} false) and end up converged to the new
 *       primary's state - the injected divergence is gone.</li>
 * </ul>
 *
 * RS bootstrap / wire / SCRAM helpers follow {@link UserFailoverTest}. Expected log noise:
 * "Duplicate _id" WARN/ERROR lines can appear while buffered events are replayed idempotently
 * on top of matching data - handled by the idempotent fallback, not a failure signal.
 */
@Tag("server")
public class FastResyncTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private static final String DB = "fastresyncdb";
    private static final String COLL = "objs";
    private static final int DOCS = 50;

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

    // ---- RS bootstrap helpers (pattern of UserFailoverTest) --------------------------------

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
        return false;
    }

    // ---- wire helpers ----------------------------------------------------------------------

    /** Send one OP_MSG command over a raw socket to a node and return the reply's first document. */
    private Map<String, Object> command(int port, Map<String, Object> cmd) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port), 2000);
            sock.setSoTimeout(15_000);
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

    private double okOf(Map<String, Object> reply) {
        Object v = reply.get("ok");
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    /** Real SCRAM client login against ONE specific node (UserFailoverTest's helper). */
    private boolean scramLoginWorks(int port, String user, String password) {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        SingleMongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", user, password);
        try {
            con.connect(carrier, "localhost", port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                con.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** The combined dbHash md5 of one database on one node (over the wire), null on error. */
    private Object dbHashMd5(int port, String db) throws Exception {
        Map<String, Object> reply = command(port, Doc.of("dbHash", 1, "$db", db));
        return okOf(reply) == 1.0 ? reply.get("md5") : null;
    }

    /** The dbHash of admin restricted to system.users (the only replicated admin collection). */
    private Object usersHash(int port) throws Exception {
        Map<String, Object> reply = command(port,
                Doc.of("dbHash", 1, "collections", List.of("system.users"), "$db", "admin"));
        return okOf(reply) == 1.0 ? reply.get("md5") : null;
    }

    // ---- common bootstrap ------------------------------------------------------------------

    private record Cluster(PoppyDB node1, PoppyDB node2, PoppyDB node3,
                           int port1, int port2, int port3) {}

    /**
     * Start a 3-node election RS with deterministic priorities (node1 wins, node2 is the
     * designated survivor-primary), create a user and {@link #DOCS} documents on node1, and
     * wait until node2 and node3 both hold them AND agree on the dbHash of the data db and
     * admin.system.users - so the shortcut precondition ("survivors are identical") is a
     * fact, not a hope, before any test kills the primary.
     */
    private Cluster bootstrapClusterWithData() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        // Distinct, strictly ordered priorities within the election-mode 0-100 clamp (see
        // UserFailoverTest): node1 wins the initial election, node2 the re-election.
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 50,
                          "localhost:" + port3, 10);
        node1.configureReplicaSet("rsFastResync", hosts, prio, true, null);
        node2.configureReplicaSet("rsFastResync", hosts, prio, true, null);
        node3.configureReplicaSet("rsFastResync", hosts, prio, true, null);

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        Map<String, Object> createReply = command(port1, Doc.of(
                "createUser", "fast-user", "pwd", "fast-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(createReply), "createUser on the primary must succeed: " + createReply);

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < DOCS; i++) {
            docs.add(Doc.of("_id", "doc-" + i, "counter", i, "strValue", "value-" + i));
        }
        Map<String, Object> insertReply = command(port1, Doc.of(
                "insert", COLL, "documents", docs, "$db", DB));
        assertEquals(1.0, okOf(insertReply), "insert on the primary must succeed: " + insertReply);

        // Converged: counts, logins, and (the strong form) identical dbHashes on both survivors.
        assertTrue(poll(30_000, () -> node2.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS
                && node3.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS),
                "data must replicate to both secondaries");
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "fast-user", "fast-pw")
                && scramLoginWorks(port3, "fast-user", "fast-pw")),
                "user must replicate to both secondaries");
        assertTrue(poll(30_000, () -> {
            Object h2 = dbHashMd5(port2, DB);
            Object h3 = dbHashMd5(port3, DB);
            Object u2 = usersHash(port2);
            Object u3 = usersHash(port3);
            return h2 != null && h2.equals(h3) && u2 != null && u2.equals(u3);
        }), "both survivors must agree on data and user hashes before the failover");

        return new Cluster(node1, node2, node3, port1, port2, port3);
    }

    // ---- tests -----------------------------------------------------------------------------

    @Test
    public void shortcutTakenWhenDataIdentical() throws Exception {
        Cluster c = bootstrapClusterWithData();

        // The RM node3 is running right now targets node1; after the kill it gets REPLACED by a
        // fresh one targeting node2. Capture the old instance so the poll below cannot be
        // satisfied by the pre-failover manager (whose own sync may also have been a trivial
        // empty-vs-empty shortcut at bootstrap).
        ReplicationManager rmBefore = c.node3().getReplicationManagerForTest();

        c.node1().shutdown(); // forcing failover
        nodes.remove(c.node1());
        waitForPrimary(c.node2());

        // The crux: the re-targeted follower must converge via the consistency shortcut, not a
        // full wipe + re-copy.
        assertTrue(poll(30_000, () -> {
            ReplicationManager rm = c.node3().getReplicationManagerForTest();
            return rm != null && rm != rmBefore && rm.isInitialSyncComplete()
                    && rm.wasLastSyncShortcut();
        }), "re-targeted follower with identical data must complete its sync via the shortcut");

        // Data and users intact after the shortcut.
        assertEquals(DOCS, c.node3().getDriver().count(DB, COLL, Doc.of(), null, null),
                "all documents must still be present on the shortcut-synced follower");
        assertTrue(poll(15_000, () -> scramLoginWorks(c.port3(), "fast-user", "fast-pw")),
                "the pre-failover user must remain loginable on the shortcut-synced follower");
        assertTrue(scramLoginWorks(c.port2(), "fast-user", "fast-pw"),
                "the pre-failover user must be loginable on the new primary");

        // And the follower must still be a live replica: a write on the new primary reaches it.
        Map<String, Object> postReply = command(c.port2(), Doc.of(
                "insert", COLL, "documents", List.of(Doc.of("_id", "post-failover", "counter", -1)),
                "$db", DB));
        assertEquals(1.0, okOf(postReply), "insert on the new primary must succeed: " + postReply);
        assertTrue(poll(30_000, () -> c.node3().getDriver()
                .count(DB, COLL, Doc.of("_id", "post-failover"), null, null) == 1),
                "a write on the new primary must replicate to the shortcut-synced follower");
    }

    @Test
    public void fallbackOnDivergence() throws Exception {
        Cluster c = bootstrapClusterWithData();

        // Test-only backdoor: write straight into node3's InMemoryDriver, bypassing replication,
        // to simulate divergence (e.g. lost/extra local state). The RM never sees this write.
        GenericCommand inject = new GenericCommand(c.node3().getDriver());
        inject.setDb(DB);
        inject.setColl(COLL);
        inject.setCmdData(Doc.of(
                "insert", COLL, "$db", DB,
                "documents", List.of(Doc.of("_id", "diverged-doc", "counter", -42))));
        c.node3().getDriver().runCommand(inject);
        assertEquals(DOCS + 1, c.node3().getDriver().count(DB, COLL, Doc.of(), null, null),
                "the injected divergence must be present before the failover");

        ReplicationManager rmBefore = c.node3().getReplicationManagerForTest();

        c.node1().shutdown(); // forcing failover
        nodes.remove(c.node1());
        waitForPrimary(c.node2());

        // The fresh RM must detect the mismatch and run today's full clear + snapshot.
        assertTrue(poll(30_000, () -> {
            ReplicationManager rm = c.node3().getReplicationManagerForTest();
            return rm != null && rm != rmBefore && rm.isInitialSyncComplete();
        }), "re-targeted follower must complete a sync after the failover");

        ReplicationManager rm = c.node3().getReplicationManagerForTest();
        assertFalse(Objects.requireNonNull(rm).wasLastSyncShortcut(),
                "a diverged follower must NOT take the consistency shortcut");

        // Full sync converged the follower to the new primary's state: the injected document is
        // gone, the real data and the user survived.
        assertTrue(poll(30_000, () -> c.node3().getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS
                && c.node3().getDriver().count(DB, COLL, Doc.of("_id", "diverged-doc"), null, null) == 0),
                "full sync must remove the injected divergence and restore the primary's state");
        assertTrue(poll(15_000, () -> scramLoginWorks(c.port3(), "fast-user", "fast-pw")),
                "the user must still be loginable on the fully re-synced follower");
    }
}
