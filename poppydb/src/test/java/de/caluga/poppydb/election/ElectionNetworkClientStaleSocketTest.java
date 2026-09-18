package de.caluga.poppydb.election;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.poppydb.PoppyDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A peer connection the PEER has closed (its idle timeout - a follower's election connections
 * idle whenever it is neither leader nor candidate) must not cost an election round.
 *
 * <p>Before: the driver did not notice the close ({@code isConnected()} stayed true, and an EOF
 * on the reply read came back as a plain {@code null}), so the first vote request after the
 * failover vanished ("Null response"), the round was lost, and the next election timeout paid
 * for it - 3-5s per round at FastResyncTest's timers, more under load. Seen in every captured
 * slow failover: "Null response from localhost:NNN for vote request" for a peer that was alive
 * and answering everyone else.
 */
public class ElectionNetworkClientStaleSocketTest {

    private PoppyDB peer;
    private ElectionNetworkClient client;

    @AfterEach
    void cleanup() {
        if (client != null) {
            client.stop();
            client = null;
        }
        if (peer != null) {
            try {
                peer.shutdown();
            } catch (Exception e) {
                // best effort
            }
            peer = null;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Peer with a 1s idle timeout; a warmed connection, left alone for 2.5s, is closed by the peer. */
    private String idleClosedPeerConnection() throws Exception {
        int peerPort = freePort();
        String peerAddress = "localhost:" + peerPort;
        peer = new PoppyDB(peerPort, "localhost", 100, 1);
        peer.start();
        ElectionManager em = new ElectionManager("localhost:1", List.of("localhost:1", peerAddress),
                new ElectionConfig());
        client = new ElectionNetworkClient(em);
        client.start();
        assertNotNull(client.getOrCreateConnection(peerAddress), "precondition: the peer is reachable");
        Thread.sleep(2500);
        return peerAddress;
    }

    @Test
    void commandStillReachesThePeerOverAnIdleClosedConnection() throws Exception {
        String peerAddress = idleClosedPeerConnection();

        Map<String, Object> reply = client.exchange(peerAddress, Doc.of("ping", 1));

        assertNotNull(reply, "a peer that closed its idle connection is still reachable - redial, do not lose the round");
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue(), "the peer must have answered: " + reply);
    }

    /**
     * The driver-level half, deterministic: a raw connection whose peer closed the socket reads
     * EOF (no write first, so no reset - a reset takes the exception path and the driver's
     * retry helper reconnects, which is fine). Before the fix, EOF came back as a plain
     * {@code null} and the connection stayed marked connected.
     */
    @Test
    void eofOnTheReplyReadClosesTheConnection() throws Exception {
        int peerPort = freePort();
        peer = new PoppyDB(peerPort, "localhost", 100, 1);
        peer.start();

        SingleMongoConnectDriver settings = new SingleMongoConnectDriver();
        settings.setHostSeed("localhost:" + peerPort);
        settings.setConnectionTimeout(1000);
        settings.setMaxWaitTime(1000);
        SingleMongoConnection raw = new SingleMongoConnection();
        raw.connect(settings, "localhost", peerPort);
        assertTrue(raw.isConnected(), "precondition: the handshake went through");

        Thread.sleep(2500); // past the peer's 1s idle timeout - it has closed the socket by now

        Object reply = raw.readNextMessage(1000);

        assertNull(reply, "nothing arrives on a socket the peer closed");
        assertFalse(raw.isConnected(),
                "an EOF on the read must close the connection - a driver that still reports "
                        + "connected would hand the same dead socket to the next request");
        raw.close();
    }
}
