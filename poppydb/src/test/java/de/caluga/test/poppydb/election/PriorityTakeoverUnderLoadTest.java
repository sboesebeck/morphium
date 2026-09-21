package de.caluga.test.poppydb.election;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #388, the scenario from the test runner end to end: priorities 100/75/50, the primary (100)
 * is stepped down for 15s and frozen, a lower-priority node takes over, the preferred node is
 * unfrozen and its stepdown block expires - and all the while clients keep writing. The leader
 * must yield to the preferred node once it is electable and caught up, within a check interval
 * plus an election, and the writes must continue through the handover.
 *
 * <p>This exercises the whole production path of the fix - the follower's position travelling
 * in the heartbeat response (wired in PoppyDB), the freshness rule, the withheld-reason
 * bookkeeping - against real nodes; the rule-level cases live in {@link PriorityTakeoverTest}.
 * Timers are scaled down (5s stability, 1s check) to keep the test short.
 */
@Tag("server")
public class PriorityTakeoverUnderLoadTest {

    private static final Logger log = LoggerFactory.getLogger(PriorityTakeoverUnderLoadTest.class);
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final List<PoppyDB> nodes = new ArrayList<>();
    private static final int WRITERS = 2;

    private final AtomicBoolean writing = new AtomicBoolean(false);
    private final List<Thread> writers = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        writing.set(false);
        for (Thread writer : writers) {
            writer.join(5000);
        }
        writers.clear();
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

    /** One instance per node - configureReplicaSet stores the node's own priority in it. */
    private static ElectionConfig takeoverAtTestSpeed() {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000)
                .setHeartbeatIntervalMs(250)
                .setPriorityTakeoverEnabled(true)
                .setPriorityTakeoverCheckIntervalMs(1000)
                .setPriorityTakeoverMinStabilityMs(5000)
                .setPriorityTakeoverStepDownSecs(5);
    }

    private PoppyDB currentPrimary() {
        for (PoppyDB n : nodes) {
            if (n.isPrimary()) {
                return n;
            }
        }
        return null;
    }

    private boolean awaitPrimary(PoppyDB node, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isPrimary()) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private PoppyDB awaitAnyPrimaryExcept(PoppyDB excluded, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (PoppyDB n : nodes) {
                if (n != excluded && n.isPrimary()) {
                    return n;
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    /** One OP_MSG command over an open socket. */
    private static Map<String, Object> command(Socket sock, Map<String, Object> cmd) throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(MSG_ID.incrementAndGet());
        msg.setFlags(0);
        msg.setFirstDoc(cmd);
        sock.getOutputStream().write(msg.bytes());
        sock.getOutputStream().flush();
        OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
        return reply.getFirstDoc();
    }

    /**
     * {@link #WRITERS} threads insert documents into whichever node is primary, a few hundred
     * per second in total, reconnecting on every error - the load a busy messaging client puts
     * on the set. Deliberately not a firehose: at ~25.000 inserts/s the demoted node's full
     * resync (forced by its non-replicated collections) never drains its backlog, it is then
     * GENUINELY behind and withholding the takeover is right. Each thread runs the loop below.
     */
    private void startWriters(AtomicLong written) {
        writing.set(true);
        for (int i = 0; i < WRITERS; i++) {
            Thread writer = new Thread(() -> writerLoop(written), "takeover-load-writer-" + i);
            writer.setDaemon(true);
            writers.add(writer);
            writer.start();
        }
    }

    private void writerLoop(AtomicLong written) {
        Socket sock = null;
        try {
            while (writing.get()) {
                try {
                    if (sock == null) {
                        PoppyDB primary = currentPrimary();
                        if (primary == null) {
                            Thread.sleep(50);
                            continue;
                        }
                        sock = new Socket();
                        sock.connect(new InetSocketAddress("localhost", primary.getPort()), 1000);
                        sock.setSoTimeout(5000);
                    }
                    Map<String, Object> reply = command(sock, Doc.of(
                            "insert", "load", "documents", List.of(Doc.of("n", written.get(), "t", System.currentTimeMillis())),
                            "$db", "takeover_load"));
                    Object ok = reply.get("ok");
                    if (ok instanceof Number n && n.doubleValue() == 1.0) {
                        written.incrementAndGet();
                    } else {
                        sock.close();
                        sock = null;
                    }
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    try {
                        if (sock != null) sock.close();
                    } catch (Exception ignored) {
                    }
                    sock = null;
                }
            }
        } finally {
            try {
                if (sock != null) sock.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void preferredNodeTakesOverUnderContinuousWrites() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(hosts.get(0), 100, hosts.get(1), 75, hosts.get(2), 50);

        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        node1.configureReplicaSet("rsLoad", hosts, prio, true, takeoverAtTestSpeed());
        node2.configureReplicaSet("rsLoad", hosts, prio, true, takeoverAtTestSpeed());
        node3.configureReplicaSet("rsLoad", hosts, prio, true, takeoverAtTestSpeed());
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        assertTrue(awaitPrimary(node1, 30_000), "the preferred node must win the initial election");

        AtomicLong written = new AtomicLong();
        startWriters(written);
        long until = System.currentTimeMillis() + 10_000;
        while (written.get() < 100 && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }
        assertTrue(written.get() >= 100, "the write load must be running: " + written.get());

        // Both followers must be past their initial sync before the primary goes: a follower
        // whose data is incomplete holds back its candidacy, and with the old primary blocked
        // and frozen the set would be stuck leaderless - a different problem from #388.
        until = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < until
                && !(node2.getElectionManager().isDataComplete() && node3.getElectionManager().isDataComplete())) {
            Thread.sleep(50);
        }
        assertTrue(node2.getElectionManager().isDataComplete() && node3.getElectionManager().isDataComplete(),
                "both followers must have completed their initial sync");
        Thread.sleep(3000);  // and replicate the live stream for a while
        log.info("followers synced, {} documents written", written.get());

        // The runner's sequence: stepdown 15s + freeze, another node takes over, the preferred
        // node is unfrozen a few seconds later and becomes electable when its block expires.
        ElectionManager em1 = node1.getElectionManager();
        long stepdownAt = System.currentTimeMillis();
        assertTrue(em1.stepDown(15, 0, true), "stepdown should succeed");
        em1.freeze(60);

        PoppyDB temporary = awaitAnyPrimaryExcept(node1, 30_000);
        assertNotNull(temporary, "a lower-priority node must take over");
        log.info("temporary primary: priority {} after {}ms, {} documents written so far",
                prio.get(temporary.getHost() + ":" + temporary.getPort()), System.currentTimeMillis() - stepdownAt, written.get());

        Thread.sleep(5000);
        em1.unfreeze();
        long blockExpiresAt = stepdownAt + 15_000;
        while (System.currentTimeMillis() < blockExpiresAt || em1.isElectionBlocked()) {
            Thread.sleep(100);
        }
        log.info("preferred node electable again after {}ms, {} documents written",
                System.currentTimeMillis() - stepdownAt, written.get());

        // stability (5s) has long passed for the temporary leader; one check interval (1s) plus
        // an election (1-2s) plus generous slack for a loaded machine
        long writtenBefore = written.get();
        boolean reclaimed = awaitPrimary(node1, 20_000);
        log.info("takeover {}: {} documents written while waiting, {} in total; temporary leader stats: {}",
                reclaimed ? "happened" : "DID NOT happen", written.get() - writtenBefore, written.get(),
                temporary.getElectionManager().getStats().get("takeoverWithheld"));
        assertTrue(reclaimed, "the preferred node (priority 100), electable and keeping pace with the writes, must be handed "
                + "leadership within 20s of becoming electable - takeover withheld: "
                + temporary.getElectionManager().getStats().get("takeoverWithheld"));
        assertTrue(written.get() > writtenBefore, "writes must have continued through the handover");
    }
}
