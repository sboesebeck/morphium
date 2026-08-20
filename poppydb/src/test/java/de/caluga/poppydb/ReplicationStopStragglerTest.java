package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

/**
 * #323 (cheap half): {@code ReplicationManager.stop()} joins the sync thread with a 5s bound and
 * then proceeds - but the sync connection uses a 60s read timeout, socket reads are not
 * interruptible, and {@code performInitialSync}/{@code syncCollection} checked neither
 * {@code running} nor interruption between collections. The abandoned thread kept inserting
 * documents FROM THE OLD PRIMARY into the local driver while a replacement ReplicationManager was
 * already doing its own clear-and-copy - stale foreign documents, silent divergence.
 *
 * <p>Cheap half fixed here: cooperative cancellation (no local write after {@code stop()}, checks
 * between databases/collections and, crucially, between a completed read and its local insert)
 * plus closing the tracked in-flight sync connection on {@code stop()} so a blocked socket read
 * aborts instead of running out its 60s. The deterministic slow-primary read seam, the
 * generation-check-before-every-write belt-and-suspenders and the convergence-after-chaos test
 * remain in #324/6.4.0 scope.
 *
 * <p>The test parks the sync thread via a deliberately UNINTERRUPTIBLE pause inside
 * {@code syncCollection} (between taking the read connection and the local insert) - mirroring a
 * socket read on a slow primary, which {@code Thread.interrupt()} cannot unblock either. That is
 * exactly the state in which {@code stop()}'s join loses.
 */
@Tag("server")
public class ReplicationStopStragglerTest {

    private static final String DB = "stragglertest";

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
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

            Thread.sleep(50);
        }

        return Boolean.TRUE.equals(condition.call());
    }

    private long localDocCount() throws Exception {
        long total = 0;

        for (int c = 0; c < 5; c++) {
            total += local.count(DB, "coll" + c, Doc.of(), null, null);
        }

        return total;
    }

    /**
     * A sync thread that stop() abandoned mid-read must never write into the local driver once
     * it unblocks - before the fix it happily completed the read and inserted every remaining
     * collection into data that by then belongs to its successor.
     */
    @Test
    public void aStoppedSyncThreadMustNotWriteIntoItsSuccessorsData() throws Exception {
        int port = nextPort();
        PoppyDB primary = startStandalonePrimary(port);

        for (int c = 0; c < 5; c++) {
            for (int i = 0; i < 3; i++) {
                primary.getDriver().store(DB, "coll" + c,
                        List.of(Doc.of("_id", "d" + c + "-" + i, "v", i)), null);
            }
        }

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port);
        // Skip the consistency shortcut so the cycle goes straight into the full snapshot,
        // whose per-collection loop carries the seam.
        rm.setWipedThisSyncCycleForTest(true);
        rm.armSyncReadPauseForTest();
        rm.start();

        assertTrue(poll(15_000, rm::syncReadPauseReachedForTest),
                "the sync thread must be parked inside syncCollection's read");
        Thread syncThread = rm.getInitialSyncThreadForTest();
        assertNotNull(syncThread, "sanity: the initial-sync thread must exist");

        // stop() interrupts and joins (bounded) - the parked thread ignores the interrupt,
        // exactly like a blocked socket read would, so the join loses and stop() proceeds.
        rm.stop();
        assertTrue(syncThread.isAlive(),
                "precondition: the sync thread survived stop() (parked in the read, join lost)");
        long docsAtStop = localDocCount();

        // The straggler unblocks - the read "completes". It must now abandon the cycle instead
        // of inserting the read result (and every further collection) into its successor's data.
        rm.releaseSyncReadPauseForTest();
        syncThread.join(10_000);

        assertFalse(syncThread.isAlive(), "the straggler must terminate once it unblocks");
        assertTrue(localDocCount() == docsAtStop,
                "a stopped sync thread must not write into the local driver after stop() "
                        + "(had " + docsAtStop + " docs at stop, now " + localDocCount() + ")");
    }

    /**
     * stop() must close the tracked in-flight sync connection: for a REAL blocked socket read
     * (not this seam) that close is the only thing that ends the read before its 60s timeout.
     */
    @Test
    public void stopClosesTheInFlightSyncConnection() throws Exception {
        int port = nextPort();
        PoppyDB primary = startStandalonePrimary(port);
        primary.getDriver().store(DB, "coll0", List.of(Doc.of("_id", "d0", "v", 0)), null);

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port);
        rm.setWipedThisSyncCycleForTest(true);
        rm.armSyncReadPauseForTest();
        rm.start();

        assertTrue(poll(15_000, rm::syncReadPauseReachedForTest),
                "the sync thread must be parked inside syncCollection's read");

        MongoConnection inFlight = rm.getInFlightSyncConnectionForTest();
        assertNotNull(inFlight, "the in-flight sync connection must be tracked while a "
                + "collection read is in progress");
        assertTrue(inFlight.isConnected(), "sanity: the tracked connection is live before stop()");

        rm.stop();

        assertFalse(inFlight.isConnected(),
                "stop() must close the in-flight sync connection - it is the only way a socket "
                        + "read blocked on a slow primary ends before its 60s timeout");
    }
}
