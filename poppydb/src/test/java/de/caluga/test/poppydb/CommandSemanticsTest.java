package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.PoppyDB;
import de.caluga.test.mongo.suite.base.TestUtils;

/**
 * Verifies that the command middleware (primary rejection, read preference, transaction
 * context and write concern) runs for the fast-path direct-dispatch commands the same way
 * it runs for the generic {@code processDefaultCommandAsync} path.
 *
 * <p>The commands are issued over raw OP_MSG sockets on purpose: this hits the direct
 * dispatch executors (processInsertDirect / processCountDirect) exactly, and lets us read
 * the raw server reply (error code / writeConcernError) without a smart driver redirecting
 * writes to the primary or auto-draining cursors.
 */
@Tag("server")
@Disabled("Disabled by default - runs local PoppyDB replica sets which are flaky under parallel tests. Run manually or with --include-tags server")
public class CommandSemanticsTest {
    private static final Logger log = LoggerFactory.getLogger(CommandSemanticsTest.class);
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Send one OP_MSG command over a raw socket and return the reply's first document. */
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

    private Socket connect(int port) throws Exception {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        s.setSoTimeout(15000);
        return s;
    }

    private double ok(Map<String, Object> reply) {
        Object v = reply.get("ok");
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    private List<PoppyDB> startReplicaSet(int... ports) throws Exception {
        List<String> hosts = new java.util.ArrayList<>();
        for (int p : ports) hosts.add("127.0.0.1:" + p);
        List<PoppyDB> servers = new java.util.ArrayList<>();
        for (int p : ports) servers.add(new PoppyDB(p, "127.0.0.1", 1000, 10));
        for (PoppyDB srv : servers) {
            srv.configureReplicaSet("rs_semantics", hosts, null, true, null);
        }
        for (PoppyDB srv : servers) {
            srv.start();
        }
        TestUtils.waitForConditionToBecomeTrue(20000, "No primary elected", () -> {
            for (PoppyDB srv : servers) {
                if (srv.isPrimary()) return true;
            }
            return false;
        });
        return servers;
    }

    private void shutdownAll(List<PoppyDB> servers) {
        for (PoppyDB srv : servers) {
            try {
                if (srv.isRunning()) srv.shutdown();
            } catch (Exception e) {
                log.warn("shutdown error: {}", e.getMessage());
            }
        }
    }

    // Scenario 1: a secondary must reject a fast-path (direct-dispatch) insert.
    @Test
    public void secondaryRejectsDirectInsert() throws Exception {
        List<PoppyDB> servers = startReplicaSet(freePort(), freePort(), freePort());
        try {
            PoppyDB secondary = servers.stream().filter(s -> !s.isPrimary()).findFirst()
                    .orElseThrow(() -> new AssertionError("no secondary found"));
            log.info("Using secondary on port {}", secondary.getPort());

            try (Socket sock = connect(secondary.getPort())) {
                Map<String, Object> reply = command(sock, Doc.of(
                        "insert", "sem_coll",
                        "documents", List.of(Doc.of("_id", 1, "v", "x")),
                        "$db", "semtest"));
                log.info("Insert-on-secondary reply: {}", reply);

                assertEquals(0.0, ok(reply), "secondary must reject the write (ok:0)");
                Object code = reply.get("code");
                assertNotNull(code, "reply must carry a not-primary error code");
                assertEquals(10107, ((Number) code).intValue(), "expected NotWritablePrimary (10107)");

                // The secondary data must be unchanged.
                Map<String, Object> count = command(sock, Doc.of(
                        "count", "sem_coll", "query", Doc.of(), "$db", "semtest"));
                assertEquals(1.0, ok(count), "count must succeed on secondary");
                assertEquals(0, ((Number) count.get("n")).intValue(),
                        "rejected insert must not have written to the secondary");
            }
        } finally {
            shutdownAll(servers);
        }
    }

    // Scenario 2: write concern w>secondaries must wait for replication on the fast path
    // and surface a writeConcernError instead of an immediate OK.
    @Test
    public void writeConcernHonoredOnDirectInsert() throws Exception {
        List<PoppyDB> servers = startReplicaSet(freePort(), freePort(), freePort());
        try {
            PoppyDB primary = servers.stream().filter(PoppyDB::isPrimary).findFirst()
                    .orElseThrow(() -> new AssertionError("no primary found"));
            log.info("Using primary on port {}", primary.getPort());

            try (Socket sock = connect(primary.getPort())) {
                // w:4 can never be satisfied by a 3-node RS (only 2 secondaries), so the write
                // concern always fails. Before the fix the fast-path insert ignored write
                // concern entirely and returned an immediate OK with no writeConcernError.
                Map<String, Object> reply = command(sock, Doc.of(
                        "insert", "sem_wc",
                        "documents", List.of(Doc.of("_id", 1, "v", "x")),
                        "$db", "semtest",
                        "writeConcern", Doc.of("w", 4, "wtimeout", 500)));
                log.info("Insert-with-wc reply: {}", reply);

                assertEquals(1.0, ok(reply), "insert itself succeeds on the primary");
                assertNotNull(reply.get("writeConcernError"),
                        "unsatisfiable write concern must produce a writeConcernError on the fast path "
                        + "instead of an immediate OK");
            }
        } finally {
            shutdownAll(servers);
        }
    }

    // Scenario 3: a fast-path insert must participate in the channel's transaction and be
    // rolled back on abort.
    @Test
    public void fastPathInsertParticipatesInTransaction() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 1000, 10); // standalone primary
        srv.start();
        TestUtils.waitForConditionToBecomeTrue(10000, "server not up", srv::isRunning);
        try (Socket sock = connect(port)) {
            Map<String, Object> lsid = Doc.of("id", "sem-session-1");

            Map<String, Object> insertCmd = Doc.of(
                    "insert", "sem_tx",
                    "documents", List.of(Doc.of("_id", 1, "v", "x")),
                    "$db", "semtest",
                    "lsid", lsid,
                    "txnNumber", 1L,
                    "autocommit", false);
            insertCmd.put("startTransaction", true);
            Map<String, Object> ins = command(sock, insertCmd);
            assertEquals(1.0, ok(ins), "insert inside the transaction must be accepted");

            Map<String, Object> abort = command(sock, Doc.of(
                    "abortTransaction", 1,
                    "lsid", lsid,
                    "txnNumber", 1L,
                    "autocommit", false,
                    "$db", "admin"));
            assertEquals(1.0, ok(abort), "abortTransaction must succeed");

            Map<String, Object> count = command(sock, Doc.of(
                    "count", "sem_tx", "query", Doc.of(), "$db", "semtest"));
            assertEquals(1.0, ok(count), "count must succeed");
            assertEquals(0, ((Number) count.get("n")).intValue(),
                    "aborted fast-path insert must not persist");
        } finally {
            srv.shutdown();
        }
    }
}
