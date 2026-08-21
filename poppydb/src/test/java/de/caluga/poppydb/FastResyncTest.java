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
 * Four scenarios:
 * <ul>
 *   <li>{@link #shortcutTakenWhenDataIdentical()} - identical survivors: the re-targeted
 *       follower converges via the shortcut ({@code wasLastSyncShortcut()} true), data and
 *       users intact.</li>
 *   <li>{@link #fallbackOnDivergence()} - a follower whose local state was made to diverge
 *       (test-only backdoor write into its InMemoryDriver) must take today's full clear +
 *       snapshot path ({@code wasLastSyncShortcut()} false) and end up converged to the new
 *       primary's state - the injected divergence is gone.</li>
 *   <li>{@link #fallbackOnSystemVersionDivergence()} - the same shape as
 *       {@link #fallbackOnDivergence()}, but the injected divergence lives ONLY in
 *       {@code admin.system.version} (the users-file version-gate meta doc), not in application
 *       data or {@code system.users}. Regression coverage for the fact that the shortcut's
 *       dbHash comparison (and this test's own {@link #versionHash}) must inspect that
 *       collection too - a helper that only compared data + {@code system.users} would wave a
 *       diverged version-gate doc through the shortcut.</li>
 *   <li>{@link #shortcutOnRootlessClusterCreatesNoPhantomCollections()} - a cluster that never
 *       runs {@code createUser} (no root user, no users file) at all: {@code admin.system.users}
 *       and {@code admin.system.version} never exist on either side. Regression coverage for the
 *       resync-clear fix (commit 059535d1) - {@code clearLocalDatabases()} must use {@code drop()}
 *       rather than an empty-filter delete, or the delete's internal {@code find()} phantom-creates
 *       those collections on the resyncing secondary only, permanently diverging the namespace set
 *       the shortcut compares.</li>
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

    // JVM-wide: ports handed out to ANY test in this fork. new ServerSocket(0) can return the
    // same free port twice in a row (nothing bound it in between) - Map.of over the host list
    // then dies with "duplicate key" (seen on the loaded CI runner in FastResyncTest).
    private static final java.util.Set<Integer> handedOutPorts =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    private int nextPort() throws Exception {
        for (int i = 0; i < 100; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int p = socket.getLocalPort();

                if (handedOutPorts.add(p)) {
                    return p;
                }
            }
        }

        throw new IllegalStateException("could not allocate a fresh, distinct port");
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
            try {
                carrier.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** The combined dbHash md5 of one database on one node (over the wire), null on error. */
    private Object dbHashMd5(int port, String db) throws Exception {
        Map<String, Object> reply = command(port, Doc.of("dbHash", 1, "$db", db));
        return okOf(reply) == 1.0 ? reply.get("md5") : null;
    }

    /** The dbHash of admin restricted to system.users (one of the replicated admin collections). */
    private Object usersHash(int port) throws Exception {
        Map<String, Object> reply = command(port,
                Doc.of("dbHash", 1, "collections", List.of("system.users"), "$db", "admin"));
        return okOf(reply) == 1.0 ? reply.get("md5") : null;
    }

    /**
     * The dbHash of admin restricted to system.version (the other replicated admin collection -
     * the users-file version-gate meta doc, see {@link ReplicationManager#isReplicated}).
     * Deliberately a separate helper from {@link #usersHash}, not folded into one combined admin
     * hash: a bug converging system.users but not system.version (or vice versa) must be
     * distinguishable, and {@link #fallbackOnSystemVersionDivergence()} needs to diverge ONLY
     * this collection.
     */
    private Object versionHash(int port) throws Exception {
        Map<String, Object> reply = command(port,
                Doc.of("dbHash", 1, "collections", List.of("system.version"), "$db", "admin"));
        return okOf(reply) == 1.0 ? reply.get("md5") : null;
    }

    // ---- common bootstrap ------------------------------------------------------------------

    private record Cluster(PoppyDB node1, PoppyDB node2, PoppyDB node3,
                           int port1, int port2, int port3) {}

    /**
     * Start a 3-node election RS with deterministic priorities (node1 wins, node2 is the
     * designated survivor-primary), create a user, a version-gate meta doc in
     * admin.system.version (so it carries a real document, not an absent collection), and
     * {@link #DOCS} documents on node1. Wait until node2 and node3 both hold them AND agree on
     * the dbHash of the data db, admin.system.users AND admin.system.version - so the shortcut
     * precondition ("survivors are identical") is a fact, not a hope, before any test kills the
     * primary.
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

        // Converged: counts and logins on both survivors.
        assertTrue(poll(30_000, () -> node2.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS
                && node3.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS),
                "data must replicate to both secondaries");
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "fast-user", "fast-pw")
                && scramLoginWorks(port3, "fast-user", "fast-pw")),
                "user must replicate to both secondaries");

        // Same wire shape PoppyDB#writeAppliedUsersFileVersion uses internally for the
        // users-file version gate - written directly here (not via setBootstrapUsers(), which
        // would apply through the leadership hook) purely so admin.system.version holds a real
        // document to hash/diverge in the tests below. Deliberately AFTER the user/data
        // convergence checks above, not interleaved with the createUser call: both write to
        // admin, and this test only needs a well-defined final state, not a particular order.
        Map<String, Object> versionMetaReply = command(port1, Doc.of(
                "update", "system.version",
                "updates", List.of(Doc.of(
                        "q", Doc.of("_id", "poppydb.usersFile"),
                        "u", Doc.of("_id", "poppydb.usersFile", "appliedVersion", 1L),
                        "upsert", true, "multi", false)),
                "$db", "admin"));
        assertEquals(1.0, okOf(versionMetaReply),
                "version-gate meta doc write on the primary must succeed: " + versionMetaReply);

        // Converged (the strong form): identical dbHashes on both survivors for data, users AND
        // the version-gate meta doc.
        assertTrue(poll(30_000, () -> {
            Object h2 = dbHashMd5(port2, DB);
            Object h3 = dbHashMd5(port3, DB);
            Object u2 = usersHash(port2);
            Object u3 = usersHash(port3);
            Object v2 = versionHash(port2);
            Object v3 = versionHash(port3);
            return h2 != null && h2.equals(h3) && u2 != null && u2.equals(u3)
                    && v2 != null && v2.equals(v3);
        }), "both survivors must agree on data, user and system.version hashes before the failover");

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

    @Test
    public void fallbackOnSystemVersionDivergence() throws Exception {
        Cluster c = bootstrapClusterWithData();

        // Test-only backdoor: write straight into node3's local admin.system.version, bypassing
        // replication, so ONLY the version-gate meta collection diverges - data and
        // system.users stay identical, unlike fallbackOnDivergence(). This is exactly the case
        // the shortcut's dbHash comparison (and this test's own versionHash()) must not miss.
        GenericCommand inject = new GenericCommand(c.node3().getDriver());
        inject.setDb("admin");
        inject.setColl("system.version");
        inject.setCmdData(Doc.of(
                "insert", "system.version", "$db", "admin",
                "documents", List.of(Doc.of("_id", "diverged-version-doc", "note", "test-only"))));
        c.node3().getDriver().runCommand(inject);
        assertEquals(1, c.node3().getDriver().count("admin", "system.version",
                Doc.of("_id", "diverged-version-doc"), null, null),
                "the injected divergence must be present before the failover");

        ReplicationManager rmBefore = c.node3().getReplicationManagerForTest();

        c.node1().shutdown(); // forcing failover
        nodes.remove(c.node1());
        waitForPrimary(c.node2());

        // The fresh RM must detect the mismatch (confined to system.version - data and
        // system.users are still identical) and run today's full clear + snapshot.
        assertTrue(poll(30_000, () -> {
            ReplicationManager rm = c.node3().getReplicationManagerForTest();
            return rm != null && rm != rmBefore && rm.isInitialSyncComplete();
        }), "re-targeted follower must complete a sync after the failover");

        ReplicationManager rm = c.node3().getReplicationManagerForTest();
        assertFalse(Objects.requireNonNull(rm).wasLastSyncShortcut(),
                "a follower diverged only in system.version must NOT take the consistency shortcut");

        // Full sync converged the follower to the new primary's state: the injected meta doc is
        // gone, and system.version agrees with the new primary's hash again.
        assertTrue(poll(30_000, () -> c.node3().getDriver().count("admin", "system.version",
                Doc.of("_id", "diverged-version-doc"), null, null) == 0),
                "full sync must remove the injected system.version divergence");
        assertTrue(poll(15_000, () -> {
            Object v2 = versionHash(c.port2());
            Object v3 = versionHash(c.port3());
            return v2 != null && v2.equals(v3);
        }), "system.version must converge to the new primary's hash after the full sync");
        assertTrue(poll(15_000, () -> scramLoginWorks(c.port3(), "fast-user", "fast-pw")),
                "the user must still be loginable on the fully re-synced follower");
    }

    // ---- rootless cluster (no createUser ever ran) ------------------------------------------

    /**
     * Start a 3-node election RS exactly like {@link #bootstrapClusterWithData()} but WITHOUT
     * ever touching users: no {@link PoppyDB#setRootUser}, no {@link PoppyDB#setBootstrapUsers},
     * no {@code createUser} command - the "cluster that never ran createUser (no root user
     * configured)" scenario {@code ReplicationManager#clearLocalDatabases()} names explicitly.
     * admin.system.users and admin.system.version must never come into existence on any node.
     */
    private Cluster bootstrapRootlessClusterWithData() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 50,
                          "localhost:" + port3, 10);
        node1.configureReplicaSet("rsRootless", hosts, prio, true, null);
        node2.configureReplicaSet("rsRootless", hosts, prio, true, null);
        node3.configureReplicaSet("rsRootless", hosts, prio, true, null);

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < DOCS; i++) {
            docs.add(Doc.of("_id", "doc-" + i, "counter", i, "strValue", "value-" + i));
        }
        Map<String, Object> insertReply = command(port1, Doc.of(
                "insert", COLL, "documents", docs, "$db", DB));
        assertEquals(1.0, okOf(insertReply), "insert on the primary must succeed: " + insertReply);

        assertTrue(poll(30_000, () -> node2.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS
                && node3.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS),
                "data must replicate to both secondaries");
        assertTrue(poll(30_000, () -> {
            Object h2 = dbHashMd5(port2, DB);
            Object h3 = dbHashMd5(port3, DB);
            return h2 != null && h2.equals(h3);
        }), "both survivors must agree on the data hash before the failover");

        return new Cluster(node1, node2, node3, port1, port2, port3);
    }

    @Test
    public void shortcutOnRootlessClusterCreatesNoPhantomCollections() throws Exception {
        Cluster c = bootstrapRootlessClusterWithData();

        // Precondition: nothing ever wrote to admin.system.users/system.version anywhere - a
        // fresh node with no createUser traffic never auto-vivifies them.
        assertFalse(c.node3().getDriver().listCollections("admin", null).contains("system.users"),
                "system.users must not exist before createUser ever ran");
        assertFalse(c.node3().getDriver().listCollections("admin", null).contains("system.version"),
                "system.version must not exist before the users-file feature ever ran");

        ReplicationManager rmBefore = c.node3().getReplicationManagerForTest();

        c.node1().shutdown(); // forcing failover -> resync-clear + shortcut attempt on node3
        nodes.remove(c.node1());
        waitForPrimary(c.node2());

        // The crux (commit 059535d1): clearLocalDatabases() must drop() admin.system.users/
        // system.version, never delete() them - an empty-filter delete's internal find() would
        // auto-vivify the (empty) collection on the resyncing secondary but not on the primary,
        // permanently and asymmetrically diverging the replicated namespace set the shortcut
        // compares, which would then always fall back to a full sync. Fold the phantom-free
        // check into the same poll as sync completion, so a collection that gets vivified only
        // during the sync (then never cleaned up) is caught too, not just a snapshot taken
        // after the fact.
        assertTrue(poll(30_000, () -> {
            ReplicationManager rm = c.node3().getReplicationManagerForTest();
            if (rm == null || rm == rmBefore || !rm.isInitialSyncComplete()) {
                return false;
            }
            List<String> adminColls = c.node3().getDriver().listCollections("admin", null);
            return !adminColls.contains("system.users") && !adminColls.contains("system.version");
        }), "re-targeted follower must complete a sync without phantom-creating system.users/system.version");

        // With no divergence anywhere and no phantom collections, the shortcut must be the path
        // taken - the whole point of clearing via drop() rather than delete().
        assertTrue(poll(5_000, () -> {
            ReplicationManager rm = c.node3().getReplicationManagerForTest();
            return rm != null && rm.wasLastSyncShortcut();
        }), "identical rootless survivors must converge via the shortcut, not a full resync");

        assertEquals(DOCS, c.node3().getDriver().count(DB, COLL, Doc.of(), null, null),
                "all documents must still be present on the shortcut-synced follower");
    }
}
