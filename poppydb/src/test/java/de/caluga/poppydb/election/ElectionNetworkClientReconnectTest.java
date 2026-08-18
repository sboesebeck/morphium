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
        // Started, so the matching stop() actually tears the drivers down - an unstarted client
        // used to leave its sockets and their schedulers running past the test.
        client.start();

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

    /**
     * Peer drivers must not run the driver's own recovery task (#311). That task does
     * {@code close() -> sleep -> connect()} on its own schedule, so a driver evicted while it
     * is mid-recovery reconnects afterwards and lives on as an orphan: an open socket nothing
     * references, kept healthy by that very heartbeat, possibly attached to a different RS
     * member than the peer it was created for. This client probes and redials on every
     * election heartbeat tick, so it does not need driver-side repair at all.
     */
    @Test
    void peerDriversDoNotRunTheirOwnRecoveryHeartbeat() throws Exception {
        int peerPort = freePort();
        String peerAddress = "localhost:" + peerPort;

        peer = new PoppyDB(peerPort, "localhost", 100, 60);
        peer.start();

        ElectionManager em = new ElectionManager("localhost:1", List.of("localhost:1", peerAddress),
                new ElectionConfig());
        ElectionNetworkClient client = new ElectionNetworkClient(em);
        // Started, so the matching stop() actually tears the drivers down - an unstarted client
        // used to leave its sockets and their schedulers running past the test.
        client.start();

        SingleMongoConnectDriver driver = client.getOrCreateConnection(peerAddress);

        assertNotNull(driver, "precondition: the peer is reachable");
        assertTrue(driver.getHeartbeatFrequency() >= (int) java.util.concurrent.TimeUnit.HOURS.toMillis(1),
                "the driver's own heartbeat must be effectively off, not the 2s default - "
                        + "otherwise it repairs connections behind this client's back");

        client.stop();
    }

    /**
     * A peer driver must keep talking to its one peer. {@code connect()} enlarges the host seed
     * from the hello answer to every replica-set member (the responder puts its own address
     * first), and on a failed attempt it walks to the next seed entry with
     * {@code ConnectionType.ANY}. A driver dialed for a peer that is down would then attach to
     * whatever else answers - including this very node - while the caller believes it reached
     * the peer. On 2026-08-18 that turned a candidate's vote request for a restarting peer into
     * a self-answered grant, counted under the peer's name: a 2/3 majority nobody granted.
     */
    @Test
    void peerDriverSeedStaysPinnedToTheOnePeerItWasDialedFor() throws Exception {
        int peerPort = freePort();
        String peerAddress = "localhost:" + peerPort;
        // The peer must answer hello as a replica-set member - that answer is what enlarges the
        // seed. The two other members do not have to exist; only their names travel.
        List<String> rsHosts = List.of(peerAddress, "localhost:" + freePort(), "localhost:" + freePort());

        peer = new PoppyDB(peerPort, "localhost", 100, 60);
        peer.configureReplicaSet("rs0", rsHosts, null, true, new ElectionConfig());
        peer.start();

        ElectionManager em = new ElectionManager("localhost:1", List.of("localhost:1", peerAddress),
                new ElectionConfig());
        ElectionNetworkClient client = new ElectionNetworkClient(em);
        // Started, so the matching stop() actually tears the drivers down - an unstarted client
        // used to leave its sockets and their schedulers running past the test.
        client.start();

        SingleMongoConnectDriver driver = client.getOrCreateConnection(peerAddress);

        assertNotNull(driver, "precondition: the peer is reachable");
        assertEquals(List.of(peerAddress), List.copyOf(driver.getHostSeed()),
                "the driver must not carry other replica-set members in its seed - a failed "
                        + "attempt would walk to them and silently talk to the wrong node");

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
        // Started, so the matching stop() actually tears the drivers down - an unstarted client
        // used to leave its sockets and their schedulers running past the test.
        client.start();

        SingleMongoConnectDriver dead = new SingleMongoConnectDriver();
        dead.setHostSeed(unreachable);
        client.peerConnections.put(unreachable, dead);

        SingleMongoConnectDriver got = client.getOrCreateConnection(unreachable);

        assertNotSame(dead, got, "the dead driver must not be handed out again");
        assertFalse(client.peerConnections.containsValue(dead), "the dead driver must be evicted from the cache");

        client.stop();
    }
}
