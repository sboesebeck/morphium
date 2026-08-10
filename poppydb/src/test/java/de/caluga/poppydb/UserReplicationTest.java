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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * User replication (user-replication task 3): {@code admin.system.users} is the one system
 * collection that DOES replicate, so a user created on the primary can log in against any
 * node - SCRAM verification is node-local (each node checks credentials against its own
 * admin.system.users), so a successful login on a secondary proves the user document
 * actually replicated there.
 *
 * Live replication and rotation use the 2-node RS bootstrap from
 * {@link UserWritePrimaryOnlyTest}; the late-joiner test mirrors {@link InitialSyncTest}'s
 * structure. Client logins run through morphium's real production client
 * ({@link SingleMongoConnection} with credentials - automatic SCRAM on connect), the same
 * code path {@link AuthTlsWireE2ETest} exercises.
 *
 * Everything else under admin, all of local/config and every other {@code system.*}
 * collection must stay OUT of replication - the truth-table test pins
 * {@link ReplicationManager#isReplicated(String, String)} down directly.
 */
@Tag("server")
public class UserReplicationTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

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

    // ---- RS bootstrap helpers (pattern of UserWritePrimaryOnlyTest / InitialSyncTest) ----

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

    @SuppressWarnings("unchecked")
    private boolean initialSyncComplete(PoppyDB node) {
        Object rep = node.getStats().get("replication");
        if (rep instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) rep).get("initialSyncComplete"));
        }
        return false;
    }

    private void waitForPrimary(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!node.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(node.isPrimary(), "node must become primary");
    }

    private void waitForInitialSync(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!initialSyncComplete(node) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(initialSyncComplete(node), "initial sync must complete within 60s");
    }

    /** Poll a condition with generous timeout - replication is asynchronous, never fixed-sleep. */
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

    // ---- wire helpers --------------------------------------------------------------------

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

    /**
     * Real SCRAM client login against ONE specific node (modeled on AuthTlsWireE2ETest's
     * connect): SingleMongoConnection performs the SCRAM handshake during connect, and the
     * server verifies it against its own local admin.system.users. True iff the handshake
     * succeeds.
     */
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

    // ---- tests ---------------------------------------------------------------------------

    @Test
    public void userCreatedOnPrimaryCanLoginOnSecondary() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsUserReplLive", hosts, prio);
        secondary.configureReplicaSet("rsUserReplLive", hosts, prio);

        startServer(primary, port1);
        waitForPrimary(primary);
        startServer(secondary, port2);
        waitForInitialSync(secondary);

        Map<String, Object> reply = command(port1, Doc.of(
                "createUser", "app1", "pwd", "pw1secret", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(reply), "createUser on the primary must succeed: " + reply);

        // Sanity: the login works against the node the user was created on.
        assertTrue(poll(10_000, () -> scramLoginWorks(port1, "app1", "pw1secret")),
                "SCRAM login must work on the primary itself");

        // Auth is node-local, so this only ever succeeds if the user document replicated.
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "app1", "pw1secret")),
                "user created on the primary must become loginable on the secondary");
    }

    @Test
    public void updateUserRotationReplicates() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsUserReplRotate", hosts, prio);
        secondary.configureReplicaSet("rsUserReplRotate", hosts, prio);

        startServer(primary, port1);
        waitForPrimary(primary);
        startServer(secondary, port2);
        waitForInitialSync(secondary);

        Map<String, Object> createReply = command(port1, Doc.of(
                "createUser", "app2", "pwd", "oldpw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(createReply), "createUser must succeed: " + createReply);
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "app2", "oldpw")),
                "initial password must replicate to the secondary before the rotation");

        Map<String, Object> updateReply = command(port1, Doc.of(
                "updateUser", "app2", "pwd", "newpw", "$db", "admin"));
        assertEquals(1.0, okOf(updateReply), "updateUser must succeed: " + updateReply);

        // Both conditions in the same poll round: the rotated credentials fully replaced the
        // old ones on the secondary (accepting newpw while still accepting oldpw would mean
        // a half-applied rotation).
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "app2", "newpw")
                        && !scramLoginWorks(port2, "app2", "oldpw")),
                "after updateUser the secondary must accept the new password and reject the old one");
    }

    /**
     * 2026-08-06 follow-up (dropUser): a user dropped on the primary must stop being loginable
     * on the secondary - the drop replicates as a documentKey-keyed delete event through the
     * same change stream the create/update path uses.
     */
    @Test
    public void dropUserReplicates() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsUserReplDrop", hosts, prio);
        secondary.configureReplicaSet("rsUserReplDrop", hosts, prio);

        startServer(primary, port1);
        waitForPrimary(primary);
        startServer(secondary, port2);
        waitForInitialSync(secondary);

        Map<String, Object> createReply = command(port1, Doc.of(
                "createUser", "app3", "pwd", "droppw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(createReply), "createUser must succeed: " + createReply);
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "app3", "droppw")),
                "user must replicate to the secondary before the drop");

        Map<String, Object> dropReply = command(port1, Doc.of("dropUser", "app3", "$db", "admin"));
        assertEquals(1.0, okOf(dropReply), "dropUser on the primary must succeed: " + dropReply);

        assertTrue(poll(10_000, () -> !scramLoginWorks(port1, "app3", "droppw")),
                "dropped user must stop being loginable on the primary itself");
        assertTrue(poll(30_000, () -> !scramLoginWorks(port2, "app3", "droppw")),
                "dropped user must stop being loginable on the secondary once the delete replicated");
    }

    @Test
    public void initialSyncCarriesUsers() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 300,
                          "localhost:" + port2, 100,
                          "localhost:" + port3, 50);
        node1.configureReplicaSet("rsUserReplLate", hosts, prio);
        node2.configureReplicaSet("rsUserReplLate", hosts, prio);
        node3.configureReplicaSet("rsUserReplLate", hosts, prio);

        // Only 2 of the 3 seeds are started; the user exists BEFORE node3 ever connects, so
        // the only way it can reach node3 is the initial-sync snapshot copy.
        startServer(node1, port1);
        waitForPrimary(node1);
        startServer(node2, port2);
        waitForInitialSync(node2);

        Map<String, Object> reply = command(port1, Doc.of(
                "createUser", "app3", "pwd", "pw3secret", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(reply), "createUser must succeed: " + reply);
        assertTrue(poll(10_000, () -> scramLoginWorks(port1, "app3", "pw3secret")),
                "SCRAM login must work on the primary itself");

        // Late joiner: full initial sync, then the login must work there too.
        startServer(node3, port3);
        waitForInitialSync(node3);
        assertTrue(poll(30_000, () -> scramLoginWorks(port3, "app3", "pw3secret")),
                "a late joiner must receive admin.system.users via initial sync");
    }

    /**
     * Root user creation must be primary-only in election mode (user-replication task 4): only
     * the node that {@link PoppyDB#setRootUser} is called on becomes primary, and the SECONDARY
     * has no root credentials configured at all - so it is physically incapable of self-creating "root"
     * (ensureRootUser needs rootUser/rootPassword, both null on that node). If SCRAM login for
     * "root" ever works against the secondary, the only possible explanation is that the
     * primary created it via the leadership hook and replication carried the document over.
     *
     * Election priority 0 means a node can never become a candidate (see
     * ElectionConfig#canBecomeLeaderByPriority), so the secondary can never win leadership and
     * this is deterministic, not a race between two possible primaries.
     */
    @Test
    public void rootUserIsCreatedByPrimaryAndReplicated() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        // priority 0 -> secondary can never become a candidate, let alone leader.
        var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 0);
        primary.configureReplicaSet("rsUserReplRoot", hosts, prio, true, null);
        secondary.configureReplicaSet("rsUserReplRoot", hosts, prio, true, null);

        // Only the future primary knows about root - the secondary is never given credentials,
        // so it cannot possibly self-create the user.
        primary.setRootUser("root", "rootpw");

        // Election mode needs a majority of the 2-node cluster to vote - unlike the static-mode
        // tests above, the designated primary cannot win an election alone, so both nodes must
        // be up before waiting for leadership to settle.
        startServer(primary, port1);
        startServer(secondary, port2);
        waitForPrimary(primary);
        waitForInitialSync(secondary);

        assertTrue(poll(10_000, () -> scramLoginWorks(port1, "root", "rootpw")),
                "root must be usable on the primary that created it");
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "root", "rootpw")),
                "root created by the primary's leadership hook must replicate to a secondary "
                + "that never had root credentials configured locally");
    }

    /**
     * admin.system.version is the meta-doc namespace the users-file version gate writes to
     * (user-replication task 3 follow-up): a doc inserted there on the primary must reach the
     * secondary the same way admin.system.users does, via the normal change-stream/generic-insert
     * path (not a special-cased apply). Written directly through the primary's InMemoryDriver
     * (mirroring InitialSyncTest's approach) rather than a wire command, since there is no
     * "createVersionDoc" server command - the real bootstrap apply (task 4) will use a plain
     * update-with-upsert through the driver, which fires the same insert/replace events this
     * generic insert does.
     */
    @Test
    public void systemVersionDocReplicatesToSecondary() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsSystemVersionRepl", hosts, prio);
        secondary.configureReplicaSet("rsSystemVersionRepl", hosts, prio);

        startServer(primary, port1);
        waitForPrimary(primary);
        startServer(secondary, port2);
        waitForInitialSync(secondary);

        primary.getDriver().insert("admin", "system.version",
                List.of(Doc.of("_id", "poppydb.usersFile", "appliedVersion", 1L)), null);

        assertTrue(poll(30_000, () ->
                        secondary.getDriver().count("admin", "system.version", Doc.of(), null, null) == 1),
                "a doc inserted into admin.system.version on the primary must replicate to the secondary");
    }

    /**
     * Guard: the ONLY namespace joining replication is admin.system.users. A regression that
     * lets local/config, other admin collections or other system.* collections replicate is
     * Critical (July 2026 failover semantics depend on them staying node-local).
     */
    @Test
    public void localAndConfigAndOtherSystemCollectionsStillNotReplicated() {
        assertTrue(ReplicationManager.isReplicated("admin", "system.users"),
                "admin.system.users is the one replicated system collection");
        assertTrue(ReplicationManager.isReplicated("admin", "system.version"),
                "admin.system.version replicates too - it carries the users-file version gate");

        assertFalse(ReplicationManager.isReplicated("admin", "foo"),
                "other admin collections must not replicate");
        assertFalse(ReplicationManager.isReplicated("local", "x"),
                "local must never replicate");
        assertFalse(ReplicationManager.isReplicated("local", "system.users"),
                "only ADMIN's system.users replicates - not local's");
        assertFalse(ReplicationManager.isReplicated("local", "system.version"),
                "only ADMIN's system.version replicates - not local's");
        assertFalse(ReplicationManager.isReplicated("config", "x"),
                "config must never replicate");
        assertFalse(ReplicationManager.isReplicated("config", "system.users"),
                "only ADMIN's system.users replicates - not config's");
        assertFalse(ReplicationManager.isReplicated("config", "system.version"),
                "only ADMIN's system.version replicates - not config's");
        assertFalse(ReplicationManager.isReplicated("mydb", "system.indexes"),
                "system.* in user databases must not replicate");
        assertFalse(ReplicationManager.isReplicated("mydb", "system.users"),
                "a user-db collection named system.users is still a system collection");
        assertFalse(ReplicationManager.isReplicated("mydb", "system.version"),
                "a user-db collection named system.version is still a system collection");

        assertTrue(ReplicationManager.isReplicated("mydb", "normal"),
                "normal user data must replicate");
    }
}
