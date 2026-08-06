package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * createUser/updateUser are writes (user-replication task 2): unlike plain inserts/updates,
 * they never carry {@code $fromPrimary} when applied by replication (the InMemoryDriver applies
 * them internally instead), so if a secondary accepted them straight from a client its user
 * documents would silently diverge from the primary's - the same class of bug the WRITE_COMMANDS
 * gate already prevents for inserts/updates/deletes. A secondary must therefore reject both
 * commands exactly like any other client write: {@code 10107 NotWritablePrimary}.
 *
 * 2-node PoppyDB replica set on random free ports, same bootstrap pattern as
 * {@link InitialSyncTest}: bring up node1, wait for it to become primary, bring up node2 as
 * secondary, wait for its initial sync to complete (so the RECOVERING gate at 13436 does not
 * mask the check under test), then send raw createUser/updateUser OP_MSG commands directly to
 * the secondary's socket and inspect the reply.
 */
@Tag("server")
public class UserWritePrimaryOnlyTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private PoppyDB primary;
    private PoppyDB secondary;

    @AfterEach
    public void tearDown() {
        if (secondary != null) {
            secondary.shutdown();
        }
        if (primary != null) {
            primary.shutdown();
        }
    }

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
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

    /** Send one OP_MSG command over a raw socket to a node and return the reply's first document. */
    private Map<String, Object> command(Socket sock, Map<String, Object> cmd) throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(MSG_ID.incrementAndGet());
        msg.setFlags(0);
        msg.setFirstDoc(cmd);
        sock.getOutputStream().write(msg.bytes());
        sock.getOutputStream().flush();
        OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
        return reply.getFirstDoc();
    }

    private int codeOf(Map<String, Object> reply) {
        Object c = reply.get("code");
        return c instanceof Number ? ((Number) c).intValue() : -1;
    }

    @Test
    public void secondaryRejectsCreateAndUpdateUserWithNotWritablePrimary() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        primary = new PoppyDB(port1, "localhost", 20, 5);
        secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsUserWriteOnly", hosts, prio);
        secondary.configureReplicaSet("rsUserWriteOnly", hosts, prio);

        // Bring up the primary and make sure it actually holds the primary role.
        startServer(primary, port1);
        long primaryDeadline = System.currentTimeMillis() + 15_000;
        while (!primary.isPrimary() && System.currentTimeMillis() < primaryDeadline) {
            Thread.sleep(50);
        }
        assertTrue(primary.isPrimary(), "node1 must become primary");

        // Bring up the secondary and wait for its (near-instant, empty-db) initial sync to
        // complete so the RECOVERING gate (13436) does not shadow the write-rejection check.
        startServer(secondary, port2);
        long syncDeadline = System.currentTimeMillis() + 30_000;
        while (!initialSyncComplete(secondary) && System.currentTimeMillis() < syncDeadline) {
            Thread.sleep(50);
        }
        assertTrue(initialSyncComplete(secondary), "secondary initial sync must complete within 30s");

        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port2), 2000);
            sock.setSoTimeout(15_000);

            Map<String, Object> createReply = command(sock, Doc.of(
                    "createUser", "repltestuser", "pwd", "secretpw", "roles", List.of(), "$db", "admin"));
            assertEquals(10107, codeOf(createReply),
                    "secondary must reject createUser with NotWritablePrimary: " + createReply);
            assertEquals("NotWritablePrimary", createReply.get("codeName"));

            Map<String, Object> updateReply = command(sock, Doc.of(
                    "updateUser", "repltestuser", "pwd", "othersecretpw", "$db", "admin"));
            assertEquals(10107, codeOf(updateReply),
                    "secondary must reject updateUser with NotWritablePrimary: " + updateReply);
            assertEquals("NotWritablePrimary", updateReply.get("codeName"));

            Map<String, Object> dropReply = command(sock, Doc.of(
                    "dropUser", "repltestuser", "$db", "admin"));
            assertEquals(10107, codeOf(dropReply),
                    "secondary must reject dropUser with NotWritablePrimary: " + dropReply);
            assertEquals("NotWritablePrimary", dropReply.get("codeName"));
        }
    }
}
