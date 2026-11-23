package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #530: replica-set members are not subject to the client connection limit. A node at its client
 * limit used to turn away its own set too. Member connections are now counted apart, with their
 * own fixed cap, and the client limit applies to everything else.
 *
 * <p>Every socket here comes from 127.0.0.1, so the member/client decision is made by the
 * injectable check (by the test socket's local port) wherever both kinds are needed at once;
 * the address-based default is covered by its own test.
 */
@Tag("server")
public class ConnectionLimitTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);
    private PoppyDB srv;
    private final List<Socket> sockets = new ArrayList<>();
    // local ports of the test sockets that play a replica-set member
    private final Set<Integer> memberPorts = ConcurrentHashMap.newKeySet();

    @AfterEach
    public void tearDown() {
        for (Socket s : sockets) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }

        if (srv != null) {
            srv.shutdown();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Bound before connecting, so the server-side check already knows a member's port. */
    private Socket open(int port, boolean member) throws Exception {
        Socket sock = new Socket();
        sockets.add(sock);
        sock.bind(new InetSocketAddress("127.0.0.1", 0));

        if (member) {
            memberPorts.add(sock.getLocalPort());
        }

        sock.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        sock.setSoTimeout(5000);
        return sock;
    }

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

    private static void assertHelloWorks(Socket sock) throws Exception {
        Map<String, Object> hello = command(sock, Doc.of("hello", 1, "$db", "admin"));
        assertThat(hello.get("ok")).as("hello: " + hello).isEqualTo(1.0);
    }

    /** The server closes a rejected connection right after the TCP accept. */
    private static boolean isRejected(Socket sock) throws Exception {
        sock.setSoTimeout(2000);

        try {
            return sock.getInputStream().read() == -1;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (SocketException e) {
            return true; // reset
        }
    }

    private static boolean poll(BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }

            Thread.sleep(20);
        }

        return condition.getAsBoolean();
    }

    /** Starts a standalone node; returns its port. */
    private int startServer(int maxConnections, int idleTimeoutSeconds, boolean injectedCheck) throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "127.0.0.1", maxConnections, idleTimeoutSeconds);

        if (injectedCheck) {
            srv.setMemberConnectionCheckForTest(remote -> memberPorts.contains(remote.getPort()));
        }

        srv.start();
        return port;
    }

    @Test
    public void standaloneLimitBehavesAsBefore() throws Exception {
        int port = startServer(3, 60, false);

        for (int i = 0; i < 3; i++) {
            assertHelloWorks(open(port, false));
        }

        assertThat(srv.getClientConnectionCount()).isEqualTo(3);
        assertThat(isRejected(open(port, false))).as("the 4th client is over the limit").isTrue();
        assertThat(srv.getRejectedConnectionCount()).isEqualTo(1);
        assertThat(srv.getMemberConnectionCount()).as("standalone: nothing is a member").isZero();

        // a closed client frees its slot
        sockets.get(0).close();
        assertThat(poll(() -> srv.getClientConnectionCount() == 2)).isTrue();
        assertHelloWorks(open(port, false));
        assertThat(srv.getClientConnectionCount()).isEqualTo(3);
    }

    @Test
    public void aMemberIsAcceptedAtTheClientLimit() throws Exception {
        int port = startServer(2, 60, true);

        assertHelloWorks(open(port, false));
        assertHelloWorks(open(port, false));
        assertThat(isRejected(open(port, false))).as("the 3rd client is over the limit").isTrue();

        Socket member = open(port, true);
        assertHelloWorks(member);
        assertThat(srv.getClientConnectionCount()).isEqualTo(2);
        assertThat(srv.getMemberConnectionCount()).isEqualTo(1);

        Map<String, Object> status = command(member, Doc.of("serverStatus", 1, "$db", "admin"));
        @SuppressWarnings("unchecked")
        Map<String, Object> connections = (Map<String, Object>) status.get("connections");
        assertThat(connections).as("serverStatus.connections: " + connections)
                .containsEntry("members", 1)
                .containsEntry("rejected", 1L)
                .containsKeys("current", "available", "totalCreated");
        assertThat(((Number) connections.get("current")).intValue()).isEqualTo(3);
    }

    @Test
    public void theMemberCapIsEnforced() throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "127.0.0.1", 2, 60);
        srv.maxMemberConnections = 3;
        srv.setMemberConnectionCheckForTest(remote -> true);
        srv.start();

        for (int i = 0; i < 3; i++) {
            assertHelloWorks(open(port, true));
        }

        // over the member cap a member takes a free client slot; only with both full it is rejected
        assertHelloWorks(open(port, true));
        assertHelloWorks(open(port, true));
        assertThat(isRejected(open(port, true))).as("member cap and client limit are both full").isTrue();
        assertThat(srv.getMemberConnectionCount()).isEqualTo(3);
        assertThat(srv.getClientConnectionCount()).isEqualTo(2);
        assertThat(srv.getRejectedConnectionCount()).isEqualTo(1);
    }

    /**
     * Seeds on loopback (local test clusters, an application on a member's host): every
     * connection comes from a member address. The node must admit as many connections as the
     * client limit alone did before - member cap plus --max-connections in total. The cap is
     * lowered to 4 here; the same holds for the real 256.
     */
    @Test
    public void aLoopbackSeedNeverAdmitsFewerConnectionsThanTheClientLimit() throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "127.0.0.1", 6, 60);
        srv.maxMemberConnections = 4;
        srv.configureReplicaSet("rsLoopback", List.of("127.0.0.1:" + port), null);
        srv.start();

        List<Socket> admitted = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Socket sock = open(port, false);
            assertHelloWorks(sock);
            admitted.add(sock);
        }

        assertThat(srv.getMemberConnectionCount()).isEqualTo(4);
        assertThat(srv.getClientConnectionCount()).as("the overflow counts as clients").isEqualTo(6);
        assertThat(isRejected(open(port, false))).as("member cap + client limit = 10").isTrue();
        assertThat(srv.getRejectedConnectionCount()).isEqualTo(1);

        // an overflow connection is released as a client again, the slot is reusable
        admitted.get(9).close();
        assertThat(poll(() -> srv.getClientConnectionCount() == 5)).isTrue();
        assertHelloWorks(open(port, false));
        assertThat(srv.getMemberConnectionCount()).isEqualTo(4);
        assertThat(srv.getClientConnectionCount()).isEqualTo(6);
    }

    @Test
    public void aClientClosedByTheServerFreesItsSlotExactlyOnce() throws Exception {
        int port = startServer(2, 1, false); // idle timeout 1 s: the server closes idle connections

        Socket first = open(port, false);
        Socket second = open(port, false);
        assertHelloWorks(first);
        assertHelloWorks(second);
        assertThat(srv.getClientConnectionCount()).isEqualTo(2);

        assertThat(poll(() -> srv.getClientConnectionCount() == 0)).as("closed by the idle timeout").isTrue();
        assertThat(isRejected(first)).as("the server closed it").isTrue();
        first.close();
        second.close();
        Thread.sleep(200);
        assertThat(srv.getClientConnectionCount()).as("never below zero - one decrement per channel").isZero();

        Socket third = open(port, false);
        Socket fourth = open(port, false);
        assertHelloWorks(third);
        assertHelloWorks(fourth);
        assertThat(srv.getClientConnectionCount()).isEqualTo(2);
        assertThat(isRejected(open(port, false))).as("the limit is still 2").isTrue();
    }

    /**
     * The default check: a connection from an address one of the hosts resolves to is a member.
     * A host that does not resolve leaves the others usable and makes the refresh report it.
     */
    @Test
    public void connectionsFromAHostAddressAreMembers() throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "127.0.0.1", 1, 60);
        srv.configureReplicaSet("rsConnLimit", List.of("127.0.0.1:" + port, "member.invalid:" + freePort()), null);

        assertThat(srv.refreshMemberAddresses()).as("member.invalid does not resolve").isFalse();
        assertThat(srv.isMemberAddress(InetAddress.getByName("127.0.0.1"))).isTrue();

        srv.start();

        assertHelloWorks(open(port, false));
        assertHelloWorks(open(port, false));
        assertThat(srv.getMemberConnectionCount()).as("both come from 127.0.0.1").isEqualTo(2);
        assertThat(srv.getClientConnectionCount()).isZero();
        assertThat(srv.getRejectedConnectionCount()).isZero();
    }

    @Test
    public void hostPartOfASeed() {
        assertThat(PoppyDB.hostOf("node1.example:27017")).isEqualTo("node1.example");
        assertThat(PoppyDB.hostOf("10.0.0.5:17017")).isEqualTo("10.0.0.5");
        assertThat(PoppyDB.hostOf("[::1]:17017")).isEqualTo("::1");
        assertThat(PoppyDB.hostOf("node1")).isEqualTo("node1");
        assertThat(PoppyDB.hostOf("[::1]")).isEqualTo("::1");
        assertThat(PoppyDB.hostOf("fe80::1")).isEqualTo("fe80::1");
    }
}
