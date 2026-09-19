package de.caluga.poppydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #370, the second path to a stuck guard: {@code releaseDataCompleteAfterSync} discards a
 * completion from a manager that was superseded during its sync. That is correct - its sync ran
 * against a primary that may no longer lead - but it means the node's release now depends on the
 * REPLACEMENT completing, and nothing else. This pins both halves of that contract: the
 * superseded manager can neither release nor bar the node, and the replacement's completion is
 * the release.
 */
@Tag("server")
public class SupersededSyncReleaseTest {

    private PoppyDB db;

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
                // never started - nothing to shut down
            }
        }
    }

    private ReplicationManager manager() {
        return new ReplicationManager(db.getDriver(), "127.0.0.1", 1);
    }

    @Test
    public void onlyTheCurrentManagersCompletionReleasesWhatAnEarlierOneBarred() throws Exception {
        db = new PoppyDB(freePort(), "127.0.0.1", 10, 10);
        ReplicationManager first = manager();
        db.installReplicationManagerForTest(first);

        db.onLocalDataClearedForSyncForTest(first);
        assertFalse(db.isLocalDataComplete(), "a store emptied for a sync bars candidacy (#352)");
        assertTrue(db.isLocalDataClearedForSyncForTest(), "and bars dumping");

        // Leader change: the first manager is superseded while its sync is still running.
        ReplicationManager replacement = manager();
        db.installReplicationManagerForTest(replacement);

        db.releaseDataCompleteAfterSyncForTest(first);
        assertFalse(db.isLocalDataComplete(),
                "a superseded manager's completion ran against a primary that may no longer lead - "
                        + "it must not release the guard");
        assertTrue(db.isLocalDataClearedForSyncForTest());

        db.releaseDataCompleteAfterSyncForTest(replacement);
        assertTrue(db.isLocalDataComplete(),
                "the replacement's completion is the release path - the node holds an "
                        + "authoritative copy again and may stand for election");
        assertFalse(db.isLocalDataClearedForSyncForTest(), "and may dump again");
    }

    @Test
    public void aSupersededManagerCannotBarTheNodeEither() throws Exception {
        db = new PoppyDB(freePort(), "127.0.0.1", 10, 10);
        ReplicationManager first = manager();
        db.installReplicationManagerForTest(first);
        ReplicationManager replacement = manager();
        db.installReplicationManagerForTest(replacement);

        db.onLocalDataClearedForSyncForTest(first);
        assertTrue(db.isLocalDataComplete(),
                "a manager that has been replaced cannot mark the node: the consequences outlive "
                        + "the manager, and only a completed sync would ever lift them");
        assertFalse(db.isLocalDataClearedForSyncForTest());
    }
}
