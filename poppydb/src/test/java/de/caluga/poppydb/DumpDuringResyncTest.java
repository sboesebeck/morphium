package de.caluga.poppydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #352: a node in the middle of a re-sync must not dump.
 *
 * <p>A resync resolves divergence by dropping the local databases and then copying a fresh
 * snapshot, so between those two steps the local store is legitimately empty. Over the wire that
 * window is already covered - {@code preDispatch} answers every data-plane command with
 * NotPrimaryOrSecondary (13436) while the initial sync runs. The dump path is not: it reads the
 * driver in-process, and a periodic tick landing in that window renames an <em>empty</em> file over
 * the last good dump. After that the node's persistence is emptiness, and a crash brings it back
 * empty holding a dump that looks perfectly valid - the third of the failure modes named in the
 * issue.
 *
 * <p>The same reasoning the shutdown gate in {@code dumpNow()} already spells out ("would rename
 * EMPTY databases over the last good dump files"), one window further back.
 */
@Tag("server")
public class DumpDuringResyncTest {

    /** PoppyDB that can pretend to be mid-resync and counts how often a dump was actually written. */
    private static class ResyncablePoppyDB extends PoppyDB {
        private final AtomicInteger dumpsWritten = new AtomicInteger();
        volatile boolean syncing = false;
        volatile boolean hasRm = true;

        ResyncablePoppyDB(int port) {
            super(port, "127.0.0.1", 100, 10);
        }

        @Override
        boolean isSecondarySyncing() {
            return syncing;
        }

        @Override
        boolean hasReplicationManager() {
            return hasRm;
        }

        @Override
        int writeDumpFiles() throws IOException {
            dumpsWritten.incrementAndGet();
            return super.writeDumpFiles();
        }
    }

    private ResyncablePoppyDB db;

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @AfterEach
    public void tearDown() {
        if (db != null) {
            try {
                db.shutdown();
            } catch (Exception ignored) {
                // the test never starts the server, so there may be nothing to shut down
            }
        }
    }

    private ResyncablePoppyDB serverWithDumpDir(Path dir) throws Exception {
        ResyncablePoppyDB srv = new ResyncablePoppyDB(freePort());
        srv.setDumpDirectory(dir.toFile());
        return srv;
    }

    @Test
    public void aResyncingNodeRefusesToDump(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);
        db.syncing = true;

        assertEquals(-1, db.dumpNow(),
                "a node whose local data is mid-resync must refuse the dump, not write an empty one");
        assertEquals(0, db.dumpsWritten.get(),
                "and must not reach the write path at all");
    }

    @Test
    public void aNodeWaitingForASyncRefusesToDump(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);
        db.setLocalDataComplete(false);
        db.hasRm = true;   // a replication manager exists, so a sync is coming

        assertEquals(-1, db.dumpNow(),
                "a node whose data is not authoritative and which is about to be re-synced holds "
                + "nothing worth persisting");
        assertEquals(0, db.dumpsWritten.get());
    }

    /**
     * The regression this test exists for: the first version of the guard refused whenever
     * {@code localDataComplete} was false, full stop. That flag returns to true in exactly one
     * place - {@code releaseDataCompleteAfterSync()}, driven by a ReplicationManager's initial-sync
     * completion. A standalone node or a static-mode primary has no manager and never gets that, so
     * the guard disabled its persistence for the life of the process: every write after a failed
     * restore would have existed only in memory and died with it. Worse than the empty dump the
     * guard was meant to prevent.
     */
    @Test
    public void aNodeNoSyncWillEverReachMustKeepDumping(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);
        db.setLocalDataComplete(false);
        db.hasRm = false;  // nothing will ever set the flag back

        assertTrue(db.dumpNow() >= 0,
                "refusing here would mean this node never persists anything again");
        assertEquals(1, db.dumpsWritten.get());
    }

    /**
     * H1 from the second review: the guard on the final dump was dead code. {@code shutdown()}
     * calls {@code stopReplication()} first, which nulls the ReplicationManager - so by the time
     * the guard ran, both of its manager-based inputs said "nothing is syncing" on precisely the
     * node whose store was empty. A rolling restart that stops a node mid-resync is the likeliest
     * way to reach it, and it would have persisted the emptied store as the last word.
     *
     * <p>Hence the node-level flag: "a sync emptied this store" is a property of the node, and it
     * has to outlive the manager that caused it.
     */
    @Test
    public void aNodeStoppedMidResyncDoesNotPersistItsEmptyStore(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);
        db.hasRm = false;          // as it will be by the time shutdown() reaches the final dump
        db.syncing = false;        // likewise - the manager is gone
        db.setLocalDataClearedForSyncForTest(true);

        assertEquals(-1, db.dumpNow(),
                "the emptied-store flag must survive the manager being stopped and nulled");
        assertEquals(0, db.dumpsWritten.get());
    }

    @Test
    public void theFlagIsWhatSurvivesNotTheManager(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);
        db.hasRm = false;
        db.syncing = false;
        db.setLocalDataClearedForSyncForTest(false);

        assertTrue(db.dumpNow() >= 0,
                "negative control: without the flag this is an ordinary node and must dump");
        assertEquals(1, db.dumpsWritten.get());
    }

    @Test
    public void aSyncedNodeStillDumps(@TempDir Path dir) throws Exception {
        db = serverWithDumpDir(dir);

        assertTrue(db.dumpNow() >= 0,
                "negative control: the guard must not swallow the normal case");
        assertEquals(1, db.dumpsWritten.get(),
                "a node holding complete data dumps as before");
    }
}
