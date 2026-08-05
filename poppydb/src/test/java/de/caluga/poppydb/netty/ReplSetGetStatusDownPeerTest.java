package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * {@code rs.status()} (replSetGetStatus) reported ANY non-leader peer as {@code SECONDARY},
 * regardless of whether that peer was actually reachable - a genuinely dead node stayed
 * {@code SECONDARY} forever instead of reflecting the leader's own heartbeat-tracked knowledge
 * that it had gone silent. Real MongoDB has exactly this state for this situation:
 * {@code state=8, stateStr="DOWN"}.
 */
public class ReplSetGetStatusDownPeerTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

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
        assertThat(node.isPrimary()).as("node must become primary").isTrue();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> command(int port, Map<String, Object> cmd) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port), 2000);
            sock.setSoTimeout(5000);
            OpMsg msg = new OpMsg();
            msg.setMessageId(MSG_ID.incrementAndGet());
            msg.setFirstDoc(cmd);
            sock.getOutputStream().write(msg.bytes());
            sock.getOutputStream().flush();
            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
            return reply.getFirstDoc();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> memberNamed(Map<String, Object> status, String name) {
        List<Map<String, Object>> members = (List<Map<String, Object>>) status.get("members");
        return members.stream().filter(m -> name.equals(m.get("name"))).findFirst()
                .orElseThrow(() -> new AssertionError("no member named " + name + " in " + members));
    }

    @Test
    public void deadPeerReportsDownNotSecondaryOnceHeartbeatGoesStale() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        // Short heartbeat interval so the leader picks up the follower's liveness quickly;
        // the freshness window itself has a 2000ms floor regardless (see ElectionManager -
        // isPeerReachable), so the test still needs to wait past that.
        ElectionConfig cfg = new ElectionConfig().setHeartbeatIntervalMs(100)
                .setElectionTimeoutMinMs(300).setElectionTimeoutMaxMs(500);
        PoppyDB leader = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB follower = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 100, "localhost:" + port2, 50);
        leader.configureReplicaSet("rsDownPeerTest", hosts, prio, true, cfg);
        follower.configureReplicaSet("rsDownPeerTest", hosts, prio, true, cfg);

        startServer(leader, port1);
        startServer(follower, port2);
        waitForPrimary(leader);

        // While alive, the follower must be reported as SECONDARY (the pre-existing, correct
        // case - this regression test must not flip a healthy peer to DOWN).
        long peerUpDeadline = System.currentTimeMillis() + 5000;
        String followerName = "localhost:" + port2;
        Map<String, Object> beforeShutdown;
        while (true) {
            beforeShutdown = command(port1, Doc.of("replSetGetStatus", 1, "$db", "admin"));
            if ("SECONDARY".equals(memberNamed(beforeShutdown, followerName).get("stateStr"))) {
                break;
            }
            if (System.currentTimeMillis() > peerUpDeadline) {
                throw new AssertionError("follower never reported SECONDARY while alive: "
                        + memberNamed(beforeShutdown, followerName));
            }
            Thread.sleep(100);
        }

        follower.shutdown();
        nodes.remove(follower);

        // Poll past the freshness window for the leader to notice the follower went silent.
        long deadline = System.currentTimeMillis() + 8000;
        Map<String, Object> followerMember = null;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> status = command(port1, Doc.of("replSetGetStatus", 1, "$db", "admin"));
            followerMember = memberNamed(status, followerName);
            if ("DOWN".equals(followerMember.get("stateStr"))) {
                break;
            }
            Thread.sleep(200);
        }

        assertThat(followerMember).as("dead peer never showed up as DOWN within 8s").isNotNull();
        assertThat(followerMember.get("stateStr")).isEqualTo("DOWN");
        assertThat(followerMember.get("state")).isEqualTo(8);
    }
}
