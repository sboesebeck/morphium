package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
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
@Disabled("Disabled by default - starts real PoppyDB server(s) and is flaky under parallel test runs. Run manually with -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition (see ci/CLAUDE-testvm.md).")
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

    // Scenario 2b (Task 4b / Phase C hardening): scenario 2 above only proves the NEGATIVE
    // write-concern path (an unsatisfiable w rejects with writeConcernError). This is the
    // missing POSITIVE path: on a healthy 2-node RS, w:2 must actually WAIT for the
    // secondary's ack before the client sees success - not just happen to succeed because
    // nothing failed. Proven the simple way (per the task brief): query the secondary
    // directly, synchronously, immediately after the primary's reply comes back - no sleep,
    // no poll/retry. If the fast path only waited on the primary and acked early, this read
    // would be flaky/fail because the secondary would not reliably have the document yet.
    @Test
    public void writeConcernW2WaitsForSecondaryAckOnHealthyTwoNodeSet() throws Exception {
        List<PoppyDB> servers = startReplicaSet(freePort(), freePort());
        try {
            PoppyDB primary = servers.stream().filter(PoppyDB::isPrimary).findFirst()
                    .orElseThrow(() -> new AssertionError("no primary found"));
            PoppyDB secondary = servers.stream().filter(s -> !s.isPrimary()).findFirst()
                    .orElseThrow(() -> new AssertionError("no secondary found"));
            log.info("Using primary on port {}, secondary on port {}", primary.getPort(), secondary.getPort());

            try (Socket primarySock = connect(primary.getPort());
                    Socket secondarySock = connect(secondary.getPort())) {
                Map<String, Object> reply = command(primarySock, Doc.of(
                        "insert", "sem_w2",
                        "documents", List.of(Doc.of("_id", 1, "v", "x")),
                        "$db", "semtest",
                        "writeConcern", Doc.of("w", 2, "wtimeout", 5000)));
                log.info("Insert-with-w2 reply: {}", reply);

                assertEquals(1.0, ok(reply), "insert with a satisfiable w:2 must succeed on the primary");
                assertNull(reply.get("writeConcernError"),
                        "w:2 is satisfiable on a healthy 2-node set - no writeConcernError expected, got: "
                        + reply.get("writeConcernError"));

                // The critical positive assertion: the secondary must already have the exact
                // document by the time the client received the reply - proving w:2 waited for
                // the secondary's ack rather than acking as soon as the primary wrote locally.
                Map<String, Object> secondaryCount = command(secondarySock, Doc.of(
                        "count", "sem_w2", "query", Doc.of("_id", 1), "$db", "semtest"));
                assertEquals(1.0, ok(secondaryCount), "count must succeed on secondary");
                assertEquals(1, ((Number) secondaryCount.get("n")).intValue(),
                        "w:2 ack must mean the secondary already has the document by the time the "
                        + "client receives the reply, not just the primary");
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

    // Scenario 4: the replication coordinator must be resolved live, not frozen at connect
    // time. A connection opened against a node while it is still a secondary (coordinator ==
    // null at that moment) must still honor write concern once that node is later elected
    // primary and a real coordinator is created - the pre-existing connection's handler must
    // not keep serving the stale (null) reference it captured when the pipeline was built.
    @Test
    public void writeConcernHonoredAfterLeadershipChangeOnPreExistingConnection() throws Exception {
        int p0 = freePort();
        int p1 = freePort();
        int p2 = freePort();
        List<String> hostList = List.of("127.0.0.1:" + p0, "127.0.0.1:" + p1, "127.0.0.1:" + p2);

        // Use a generous idle timeout: this test deliberately holds connections open across a
        // failover / re-election, which can take several seconds with no traffic on them.
        List<PoppyDB> servers = new ArrayList<>();
        for (int p : new int[] {p0, p1, p2}) {
            servers.add(new PoppyDB(p, "127.0.0.1", 1000, 120));
        }
        for (PoppyDB srv : servers) {
            srv.configureReplicaSet("rs_live_coord", hostList, null, true, null);
        }
        for (PoppyDB srv : servers) {
            srv.start();
        }

        Map<Integer, Socket> preElectionSockets = new HashMap<>();
        try {
            AtomicReference<PoppyDB> oldPrimaryRef = new AtomicReference<>();
            TestUtils.waitForConditionToBecomeTrue(20000, "No primary elected", () -> {
                for (PoppyDB srv : servers) {
                    if (srv.isPrimary()) {
                        oldPrimaryRef.set(srv);
                        return true;
                    }
                }
                return false;
            });
            PoppyDB oldPrimary = oldPrimaryRef.get();
            log.info("Initial primary is {}", oldPrimary.getPort());

            // Open connections to the secondaries *before* any re-election. At this moment
            // each secondary's replicationCoordinator is null, so the handler built for these
            // connections captures (or, after the fix, resolves via supplier) that null.
            List<PoppyDB> secondaries = servers.stream().filter(s -> s != oldPrimary).toList();
            for (PoppyDB s : secondaries) {
                preElectionSockets.put(s.getPort(), connect(s.getPort()));
            }

            log.info("Shutting down current primary {} to force a re-election", oldPrimary.getPort());
            oldPrimary.shutdown();

            AtomicReference<PoppyDB> newPrimaryRef = new AtomicReference<>();
            TestUtils.waitForConditionToBecomeTrue(20000, "No new primary elected after failover", () -> {
                for (PoppyDB s : secondaries) {
                    if (s.isPrimary()) {
                        newPrimaryRef.set(s);
                        return true;
                    }
                }
                return false;
            });
            PoppyDB newPrimary = newPrimaryRef.get();
            log.info("New primary is {}", newPrimary.getPort());

            Socket oldSocket = preElectionSockets.get(newPrimary.getPort());
            assertNotNull(oldSocket, "must have a pre-election connection to the new primary");

            // Unsatisfiable write concern (only 2 nodes remain) over the OLD connection. If the
            // handler still serves the coordinator reference it captured when the connection
            // was accepted (null, since the node was a secondary then), the write-concern wait
            // is silently skipped and no writeConcernError is produced.
            Map<String, Object> reply = command(oldSocket, Doc.of(
                    "insert", "sem_live_coord",
                    "documents", List.of(Doc.of("_id", 1, "v", "x")),
                    "$db", "semtest",
                    "writeConcern", Doc.of("w", 5, "wtimeout", 500)));
            log.info("Insert-with-wc reply on pre-election connection: {}", reply);

            assertEquals(1.0, ok(reply), "insert itself succeeds on the (new) primary");
            assertNotNull(reply.get("writeConcernError"),
                    "unsatisfiable write concern must still be honored on a connection that "
                    + "predates the leadership change - the coordinator must be resolved live, "
                    + "not frozen at connect time");
        } finally {
            for (Socket sock : preElectionSockets.values()) {
                try {
                    sock.close();
                } catch (Exception ignored) {
                }
            }
            shutdownAll(servers);
        }
    }

    // Scenario 5: the per-command transaction context must be cleared from the shared Netty
    // event-loop thread after each command completes. Otherwise a second connection multiplexed
    // onto the same event-loop thread inherits the first connection's open-transaction snapshot,
    // and its plain (non-transactional) write is swallowed when the first connection aborts.
    //
    // Forcing a single Netty worker thread makes both connections deterministically share one
    // event loop, so the leak (if present) reproduces every run.
    @Test
    public void plainInsertNotSwallowedByAnotherConnectionsTransaction() throws Exception {
        String prevWorkers = System.getProperty("morphiumserver.workerThreads");
        System.setProperty("morphiumserver.workerThreads", "1");
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 1000, 10); // standalone primary
        try {
            srv.start();
            TestUtils.waitForConditionToBecomeTrue(10000, "server not up", srv::isRunning);
            try (Socket txnConn = connect(port); Socket plainConn = connect(port)) {
                // Connection A opens a transaction and inserts a doc inside it (never committed).
                Map<String, Object> lsid = Doc.of("id", "sem-tx-leak");
                Map<String, Object> startTxn = Doc.of(
                        "insert", "sem_leak",
                        "documents", List.of(Doc.of("_id", "inTxn", "v", "a")),
                        "$db", "semtest",
                        "lsid", lsid,
                        "txnNumber", 1L,
                        "autocommit", false);
                startTxn.put("startTransaction", true);
                assertEquals(1.0, ok(command(txnConn, startTxn)),
                        "transactional insert must be accepted");

                // Connection B (no session) inserts a plain doc. If the event-loop thread still
                // carries connection A's transaction context, this write joins A's snapshot.
                Map<String, Object> plainInsert = command(plainConn, Doc.of(
                        "insert", "sem_leak",
                        "documents", List.of(Doc.of("_id", "plain", "v", "b")),
                        "$db", "semtest"));
                assertEquals(1.0, ok(plainInsert),
                        "plain insert on the second connection must be accepted");

                // Connection A aborts. This must discard only A's own doc, not B's plain write.
                assertEquals(1.0, ok(command(txnConn, Doc.of(
                        "abortTransaction", 1,
                        "lsid", lsid,
                        "txnNumber", 1L,
                        "autocommit", false,
                        "$db", "admin"))), "abortTransaction must succeed");

                // B's plain doc must have been committed to the real database and survive the abort.
                Map<String, Object> plainCount = command(plainConn, Doc.of(
                        "count", "sem_leak", "query", Doc.of("_id", "plain"), "$db", "semtest"));
                assertEquals(1.0, ok(plainCount), "count must succeed");
                assertEquals(1, ((Number) plainCount.get("n")).intValue(),
                        "the second connection's plain insert must be visible after the first "
                        + "connection aborts its transaction (not swallowed by a leaked tx context)");

                // A's own transactional doc must not persist after the abort.
                Map<String, Object> txnCount = command(plainConn, Doc.of(
                        "count", "sem_leak", "query", Doc.of("_id", "inTxn"), "$db", "semtest"));
                assertEquals(0, ((Number) txnCount.get("n")).intValue(),
                        "the aborted transactional insert must not persist");
            }
        } finally {
            srv.shutdown();
            if (prevWorkers == null) {
                System.clearProperty("morphiumserver.workerThreads");
            } else {
                System.setProperty("morphiumserver.workerThreads", prevWorkers);
            }
        }
    }

    // Scenario 6 (Task 6 / B2g): the aggregate command path used to swallow stage/expression
    // errors inside InMemAggregator - an unrecognized pipeline stage or an unknown $group
    // accumulator operator was logged and then quietly completed with an empty (or partial)
    // cursor. A real mongod rejects both outright. Verify PoppyDB now surfaces the same shape
    // of command error (ok:0 + errmsg + code) over the wire instead of a fake-empty result.
    @Test
    public void aggregateUnrecognizedStage_isCommandErrorNotEmptyResult() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 1000, 10); // standalone primary
        try {
            srv.start();
            TestUtils.waitForConditionToBecomeTrue(10000, "server not up", srv::isRunning);

            try (Socket sock = connect(port)) {
                assertEquals(1.0, ok(command(sock, Doc.of(
                        "insert", "sem_agg",
                        "documents", List.of(Doc.of("_id", 1, "v", 3)),
                        "$db", "semtest"))), "seed insert must succeed");

                Map<String, Object> reply = command(sock, Doc.of(
                        "aggregate", "sem_agg",
                        "pipeline", List.of(Doc.of("$notARealStage", Doc.of("x", 1))),
                        "cursor", Doc.of(),
                        "$db", "semtest"));
                log.info("Unrecognized-stage aggregate reply: {}", reply);

                assertEquals(0.0, ok(reply), "unrecognized pipeline stage must be an error, not an empty cursor");
                assertNotNull(reply.get("errmsg"), "reply must carry an error message");
                Object code = reply.get("code");
                assertNotNull(code, "reply must carry the real mongo error code");
                assertEquals(40324, ((Number) code).intValue(), "expected Unrecognized pipeline stage name (40324)");
            }
        } finally {
            srv.shutdown();
        }
    }

    // Scenario 7 (Task 1 / Phase C hardening): a secondary must reject a primary-only read
    // (readPreference mode:"primary") with the real mongod error pair - NotPrimaryNoSecondaryOk
    // (13435) - not NotWritablePrimary (10107), which is a different error reserved for rejected
    // writes (see scenario 1 above).
    @Test
    public void secondaryRejectsPrimaryReadPreferenceRead_withNotPrimaryNoSecondaryOk() throws Exception {
        List<PoppyDB> servers = startReplicaSet(freePort(), freePort(), freePort());
        try {
            PoppyDB secondary = servers.stream().filter(s -> !s.isPrimary()).findFirst()
                    .orElseThrow(() -> new AssertionError("no secondary found"));
            log.info("Using secondary on port {}", secondary.getPort());

            // "hello" is a control-plane command (bypasses preDispatch entirely), so it is a safe
            // probe for the secondary's initial-sync state. Wait until it reports secondary:true
            // (initial sync complete) - otherwise the RECOVERING check ((0) in preDispatch, added
            // in ea1baa4b) fires first and rejects with NotPrimaryOrSecondary (13436) instead of
            // reaching the readPreference check this test targets.
            TestUtils.waitForConditionToBecomeTrue(20000, "secondary did not finish initial sync", () -> {
                try (Socket probe = connect(secondary.getPort())) {
                    Map<String, Object> hello = command(probe, Doc.of("hello", 1, "$db", "admin"));
                    return Boolean.TRUE.equals(hello.get("secondary"));
                } catch (Exception e) {
                    return false;
                }
            });

            try (Socket sock = connect(secondary.getPort())) {
                Map<String, Object> reply = command(sock, Doc.of(
                        "find", "sem_rp",
                        "filter", Doc.of(),
                        "$db", "semtest",
                        "$readPreference", Doc.of("mode", "primary")));
                log.info("Primary-read-preference-on-secondary reply: {}", reply);

                assertEquals(0.0, ok(reply), "secondary must reject a primary-only read (ok:0)");
                Object code = reply.get("code");
                assertNotNull(code, "reply must carry a not-primary error code");
                assertEquals(13435, ((Number) code).intValue(), "expected NotPrimaryNoSecondaryOk (13435)");
                assertEquals("NotPrimaryNoSecondaryOk", reply.get("codeName"),
                        "codeName must match the real mongod NotPrimaryNoSecondaryOk error");
            }
        } finally {
            shutdownAll(servers);
        }
    }

    // Scenario 8 (Phase C hardening, Task 6): "explain" must be read-classified in preDispatch -
    // it is deliberately absent from both CONTROL_COMMANDS (so the primary/read-preference/
    // transaction middleware still runs for it, unlike hello/ping/election traffic) and
    // WRITE_COMMANDS (so a plain, no-readPreference explain is treated as a read and a secondary
    // may serve it directly, exactly like find/count) - see MongoCommandHandler.preDispatch. A
    // secondary rejecting this with NotWritablePrimary (10107) would mean explain got
    // misclassified as a write; this proves the opposite.
    //
    // Also exercises explain's wire-protocol round trip end to end - this is what actually reaches
    // ExplainCommand.fromMap()'s raw-map path (PoppyDB's GenericCommand dispatch), unlike the
    // driver-level ExplainCommandTest, which goes through FindCommand#explain/
    // CountMongoCommand#explain's direct field setters and never calls fromMap() at all.
    //
    // NOTE: index metadata IS replicated since #258 (initial sync + periodic 30s diff, see
    // ReplicationManager.syncIndexesFrom), but this index is created only after the secondary's
    // initial sync - relying on replication here would mean waiting for the periodic diff. The
    // index is still created directly on each node's own driver so this test's actual target -
    // explain's read-classification and wire round trip on a secondary - stays fast and focused.
    @Test
    public void secondaryServesPlainExplainAsARead() throws Exception {
        List<PoppyDB> servers = startReplicaSet(freePort(), freePort(), freePort());
        try {
            PoppyDB primary = servers.stream().filter(PoppyDB::isPrimary).findFirst()
                    .orElseThrow(() -> new AssertionError("no primary found"));
            PoppyDB secondary = servers.stream().filter(s -> !s.isPrimary()).findFirst()
                    .orElseThrow(() -> new AssertionError("no secondary found"));
            log.info("Using primary on port {}, secondary on port {}", primary.getPort(), secondary.getPort());

            TestUtils.waitForConditionToBecomeTrue(20000, "secondary did not finish initial sync", () -> {
                try (Socket probe = connect(secondary.getPort())) {
                    Map<String, Object> hello = command(probe, Doc.of("hello", 1, "$db", "admin"));
                    return Boolean.TRUE.equals(hello.get("secondary"));
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> indexKey = Doc.of("counter", 1);
            Map<String, Object> indexOptions = Doc.of("name", "counter_1");
            primary.getDriver().createIndex("semtest", "sem_explain", indexKey, indexOptions);
            secondary.getDriver().createIndex("semtest", "sem_explain", indexKey, indexOptions);

            try (Socket primarySock = connect(primary.getPort())) {
                assertEquals(1.0, ok(command(primarySock, Doc.of(
                        "insert", "sem_explain",
                        "documents", List.of(Doc.of("_id", 1, "counter", 42), Doc.of("_id", 2, "counter", 7)),
                        "$db", "semtest"))), "seed insert on primary must succeed");
            }

            // Wait for the seed documents to replicate to the secondary before explaining there.
            TestUtils.waitForConditionToBecomeTrue(20000, "seed documents never replicated to the secondary", () -> {
                try (Socket sock = connect(secondary.getPort())) {
                    Map<String, Object> count = command(sock, Doc.of("count", "sem_explain", "query", Doc.of(), "$db", "semtest"));
                    return ok(count) == 1.0 && ((Number) count.get("n")).intValue() == 2;
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> reply;
            try (Socket sock = connect(secondary.getPort())) {
                reply = command(sock, Doc.of(
                        "explain", Doc.of(
                                "find", "sem_explain",
                                "filter", Doc.of("counter", 42)),
                        "verbosity", "executionStats",
                        "$db", "semtest"));
            }
            log.info("Indexed explain-on-secondary reply: {}", reply);
            assertEquals(1.0, ok(reply), "a secondary must serve a plain (non-primary-readPreference) explain");

            @SuppressWarnings("unchecked")
            Map<String, Object> queryPlanner = (Map<String, Object>) reply.get("queryPlanner");
            assertNotNull(queryPlanner, "explain reply must carry queryPlanner");
            assertEquals("semtest.sem_explain", queryPlanner.get("namespace"),
                    "namespace must name the real wrapped collection, not the wrapped command map's toString()");

            @SuppressWarnings("unchecked")
            Map<String, Object> winningPlan = (Map<String, Object>) queryPlanner.get("winningPlan");
            assertEquals("IXSCAN", winningPlan.get("stage"));
            assertEquals("counter_1", winningPlan.get("indexName"));

            @SuppressWarnings("unchecked")
            Map<String, Object> executionStats = (Map<String, Object>) reply.get("executionStats");
            assertNotNull(executionStats, "executionStats verbosity must carry executionStats");
            assertEquals(1, ((Number) executionStats.get("nReturned")).intValue());
        } finally {
            shutdownAll(servers);
        }
    }

    @Test
    public void aggregateUnknownGroupOperator_isCommandErrorNotEmptyResult() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 1000, 10); // standalone primary
        try {
            srv.start();
            TestUtils.waitForConditionToBecomeTrue(10000, "server not up", srv::isRunning);

            try (Socket sock = connect(port)) {
                assertEquals(1.0, ok(command(sock, Doc.of(
                        "insert", "sem_agg2",
                        "documents", List.of(Doc.of("_id", 1, "v", 3)),
                        "$db", "semtest"))), "seed insert must succeed");

                Map<String, Object> reply = command(sock, Doc.of(
                        "aggregate", "sem_agg2",
                        "pipeline", List.of(Doc.of("$group", Doc.of(
                                "_id", null,
                                "broken", Doc.of("$bogusAccumulator", "$v")))),
                        "cursor", Doc.of(),
                        "$db", "semtest"));
                log.info("Unknown-group-operator aggregate reply: {}", reply);

                assertEquals(0.0, ok(reply), "unknown group operator must be an error, not an empty cursor");
                assertNotNull(reply.get("errmsg"), "reply must carry an error message");
                Object code = reply.get("code");
                assertNotNull(code, "reply must carry the real mongo error code");
                assertEquals(15952, ((Number) code).intValue(), "expected unknown group operator (15952)");
            }
        } finally {
            srv.shutdown();
        }
    }
}
