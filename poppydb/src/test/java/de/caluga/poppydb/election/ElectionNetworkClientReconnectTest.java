package de.caluga.poppydb.election;

import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.poppydb.PoppyDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A peer connection that died must be dialed again, not handed out forever.
 *
 * <p>{@link SingleMongoConnectDriver#close()} nulls its connection AND cancels its own
 * heartbeat, so such a driver never repairs itself. {@code getConnection()} then returns a
 * wrapper around {@code null} - not {@code null} - and the first use throws a plain
 * {@code RuntimeException}, which used to slip past the eviction in {@code sendCommand()}. The
 * result was a leader whose heartbeats to one peer vanished silently for hours while it kept
 * serving the other: the cut-off node saw no leader, campaigned forever, and only a restart of
 * the LEADER fixed it (ACC message-bus set, 2026-08-18).
 */
public class ElectionNetworkClientReconnectTest {

    private PoppyDB peer;

    @AfterEach
    void cleanup() {
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

    @Test
    void deadCachedConnectionIsReplacedByAFreshOne() throws Exception {
        int peerPort = freePort();
        String peerAddress = "localhost:" + peerPort;

        peer = new PoppyDB(peerPort, "localhost", 100, 60);
        peer.start();

        ElectionManager em = new ElectionManager("localhost:1", List.of("localhost:1", peerAddress),
                new ElectionConfig());
        ElectionNetworkClient client = new ElectionNetworkClient(em);

        // A driver in exactly the state the leader was stuck with: present in the cache,
        // reporting itself as not connected, and unable to recover on its own.
        SingleMongoConnectDriver dead = new SingleMongoConnectDriver();
        dead.setHostSeed(peerAddress);
        assertFalse(dead.isConnected(), "precondition: the cached driver is not connected");
        client.peerConnections.put(peerAddress, dead);

        SingleMongoConnectDriver got = client.getOrCreateConnection(peerAddress);

        assertNotNull(got, "a reachable peer must yield a connection");
        assertNotSame(dead, got, "the dead driver must not be handed out again");
        assertTrue(got.isConnected(), "the replacement must actually be connected");
        assertSame(got, client.peerConnections.get(peerAddress), "the fresh driver replaces the dead one in the cache");

        client.stop();
    }

    @Test
    void deadCachedConnectionIsEvictedEvenWhenThePeerStaysUnreachable() throws Exception {
        // Nothing listens here - the redial fails. The dead driver must still be gone, so the
        // next attempt dials again instead of reusing it.
        String unreachable = "localhost:" + freePort();

        ElectionManager em = new ElectionManager("localhost:1", List.of("localhost:1", unreachable),
                new ElectionConfig());
        ElectionNetworkClient client = new ElectionNetworkClient(em);

        SingleMongoConnectDriver dead = new SingleMongoConnectDriver();
        dead.setHostSeed(unreachable);
        client.peerConnections.put(unreachable, dead);

        SingleMongoConnectDriver got = client.getOrCreateConnection(unreachable);

        assertNotSame(dead, got, "the dead driver must not be handed out again");
        assertFalse(client.peerConnections.containsValue(dead), "the dead driver must be evicted from the cache");

        client.stop();
    }
}
