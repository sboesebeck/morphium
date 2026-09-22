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
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * #364, the initial-sync half: the sync source steps down while a follower is in the middle
 * of its snapshot copy. Every further read of the copy is answered 13435 by a node that is up
 * and will keep answering exactly that until the leadership change re-targets replication.
 * The sync thread treated that like any other failure: an ERROR with a full stack trace per
 * attempt, the regular retry schedule, and - once the same error had repeated often enough -
 * the "NODE STUCK IN RECOVERY" escalation, for a condition that is neither stuck nor an error
 * of this node. Expected instead: one WARN naming the source, the stepped-down backoff the
 * replication loop already uses, and nothing at ERROR.
 *
 * <p>The set is kept leaderless on purpose: the two other nodes are frozen before the leader
 * steps down, so the stepped-down node's hello names nobody and the driver underneath the
 * sync cannot quietly re-resolve the read to a new primary.
 */
@Tag("server")
public class SyncSourceStepdownDuringInitialSyncTest {

    private static final String DB = "stepdownsynctest";
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;
    private ch.qos.logback.classic.Logger rmLogger;
    private ListAppender<ILoggingEvent> appender;

    @AfterEach
    public void tearDown() {
        if (rmLogger != null && appender != null) {
            rmLogger.detachAppender(appender);
        }
        if (rm != null) {
            try {
                rm.releaseSyncReadPauseForTest();
            } catch (Exception ignored) {
            }
            try {
                rm.stop();
            } catch (Exception ignored) {
            }
        }
        if (local != null) {
            try {
                local.close();
            } catch (Exception ignored) {
            }
        }
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

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

    private static double okOf(Map<String, Object> reply) {
        Object v = reply.get("ok");
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    private List<ILoggingEvent> events(Level level, String containing) {
        List<ILoggingEvent> out = new ArrayList<>();
        for (ILoggingEvent ev : new ArrayList<>(appender.list)) {
            if (ev.getLevel() == level && ev.getFormattedMessage().contains(containing)) {
                out.add(ev);
            }
        }
        return out;
    }

    @Test
    @Timeout(180)
    public void aSteppedDownSyncSourceIsOneWarningNotAnErrorPerAttempt() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 50, "localhost:" + port3, 10);
        ElectionConfig cfg = new ElectionConfig().setPriorityTakeoverEnabled(false);
        node1.configureReplicaSet("rsSyncStepdown", hosts, prio, true, cfg);
        node2.configureReplicaSet("rsSyncStepdown", hosts, prio, true, cfg);
        node3.configureReplicaSet("rsSyncStepdown", hosts, prio, true, cfg);
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        assertTrue(poll(30_000, node1::isPrimary), "node1 must become primary");

        // two collections: the seam parks the copy after the first read, the second read is
        // the one that meets the stepped-down source
        for (int c = 0; c < 2; c++) {
            node1.getDriver().store(DB, "coll" + c, List.of(Doc.of("_id", "d" + c, "v", c)), null);
        }

        rmLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReplicationManager.class);
        appender = new ListAppender<>();
        appender.start();
        rmLogger.addAppender(appender);

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port1);
        rm.setWipedThisSyncCycleForTest(true);
        rm.armSyncReadPauseForTest();
        rm.start();
        assertTrue(poll(15_000, rm::syncReadPauseReachedForTest),
            "the sync thread must be parked inside syncCollection's read");

        // nobody may take over: the set stays leaderless, the stepped-down node names no primary
        for (int p : List.of(port2, port3)) {
            Map<String, Object> frozen = command(p, Doc.of("replSetFreeze", 120, "$db", "admin"));
            assertEquals(1.0, okOf(frozen), "replSetFreeze must succeed: " + frozen);
        }
        Map<String, Object> stepdown = command(port1,
            Doc.of("replSetStepDown", 120, "force", true, "$db", "admin"));
        assertEquals(1.0, okOf(stepdown), "replSetStepDown must succeed: " + stepdown);
        assertTrue(poll(5_000, () -> !node1.isPrimary()), "node1 must have stepped down");

        rm.releaseSyncReadPauseForTest();

        // the copy's next read is answered 13435; the driver's own retries end within seconds
        assertTrue(poll(60_000, () -> !events(Level.WARN, "no longer the primary").isEmpty()),
            "the sync thread must report the stepped-down source once at WARN; events so far: "
                + describe());
        // let the sync retry at least twice more (stepped-down backoff 1s, 2s, plus the
        // driver's retries inside each attempt) - the WARN must not repeat per attempt
        Thread.sleep(15_000);

        assertEquals(1, events(Level.WARN, "no longer the primary").size(),
            "one WARN per streak, not per attempt: " + describe());
        assertTrue(events(Level.ERROR, "Initial sync failed").isEmpty(),
            "a stepped-down sync source is not an ERROR: " + describe());
        assertTrue(events(Level.ERROR, "NODE STUCK IN RECOVERY").isEmpty(),
            "a leaderless window must not escalate as a stuck node: " + describe());
        assertFalse(node1.isPrimary(), "sanity: the set stayed leaderless for the whole window");
    }

    private String describe() {
        StringBuilder sb = new StringBuilder();
        for (ILoggingEvent ev : new ArrayList<>(appender.list)) {
            if (ev.getLevel().isGreaterOrEqual(Level.WARN)) {
                sb.append('\n').append(ev.getLevel()).append(' ').append(ev.getFormattedMessage());
            }
        }
        return sb.toString();
    }
}
