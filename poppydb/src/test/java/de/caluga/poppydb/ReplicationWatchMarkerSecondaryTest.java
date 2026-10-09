package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.poppydb.netty.WatchCursorManager;

/**
 * The replication-watch marker, secondary side (spec: replication flow control, step 1): the
 * {@link ReplicationManager} builds its replication watch in {@code watchForChanges()} and must
 * tag the aggregate with {@code poppyReplicationWatch: true} and {@code poppyMember} (the address
 * it also reports via {@code reportProgress}). This is the full round trip: the command as built
 * on the secondary, over the wire to a REAL primary, parsed in
 * {@code MongoCommandHandler#processChangeStream} and stored on the {@code WatchCursorState} - the
 * EmbeddedChannel tests construct the wire doc by hand, this one only exercises the command the
 * production code actually builds.
 *
 * <p>The observation point is the primary's {@code WatchCursorManager}: a watch cursor whose
 * registration carried the marker is tracked as a replication cursor there, with the secondary's
 * member address.
 */
@Tag("server")
public class ReplicationWatchMarkerSecondaryTest {

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
        if (rm != null) {
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

    private PoppyDB startStandalonePrimary(int port) throws Exception {
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
        nodes.add(srv);
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;

        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return srv;
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

            Thread.sleep(100);
        }

        return Boolean.TRUE.equals(condition.call());
    }

    @Test
    public void watchForChangesSendsTheMarkerAndTheMemberAddress() throws Exception {
        int port = nextPort();
        PoppyDB primary = startStandalonePrimary(port);

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port);
        // Generous queue so the watch reader never blocks on byte pressure during this test.
        rm.setEventQueueByteBudget(128 * 1024 * 1024);
        String member = "secondary-test-host:4711";
        rm.setMyAddress(member);
        rm.start();

        WatchCursorManager primaryCursors = primary.getCursorManagerForTest();
        assertTrue(poll(15_000, () -> !primaryCursors.replicationCursorStates().isEmpty()),
                "the primary must hold a replication watch cursor registered by the secondary");

        List<Long> replicationIds = new ArrayList<>(primaryCursors.replicationCursorStates().keySet());
        assertEquals(1, replicationIds.size(),
                "the secondary registers exactly one replication watch on the primary");
        assertTrue(primaryCursors.isReplicationCursor(replicationIds.get(0)),
                "the registered cursor must be tracked as a replication cursor");

        var backlog = primaryCursors.maxReplicationQueuedBytes();
        assertTrue(backlog.isPresent(), "the replication cursor must be visible to the backlog helper");
        assertEquals(member, backlog.get().memberAddress(),
                "the member address on the primary must be the address the secondary reports");
    }
}