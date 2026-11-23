package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * When a secondary reports its progress to the primary (#529). Progress is always reported; with
 * no progress, a heartbeat report goes out every 5 s so an idle set keeps its secondaries live -
 * but only once the initial sync is complete. Before that the node has applied nothing: counting
 * it as live would switch off the primary's 100 ms "no secondaries" answer and make every
 * w&gt;=2 write during the initial sync wait its full timeout.
 */
public class ReplicationProgressHeartbeatTest {

    private static final long HEARTBEAT = ReplicationManager.PROGRESS_HEARTBEAT_INTERVAL_MS;

    @Test
    public void progressIsAlwaysReported() {
        assertTrue(ReplicationManager.shouldReportProgress(5, 4, true, 1_000, 1_000));
        assertTrue(ReplicationManager.shouldReportProgress(5, 4, false, 1_000, 1_000));
    }

    @Test
    public void anIdleSyncedSecondaryReportsEveryFiveSeconds() {
        long now = 100_000;
        assertFalse(ReplicationManager.shouldReportProgress(5, 5, true, now, now - HEARTBEAT + 1));
        assertTrue(ReplicationManager.shouldReportProgress(5, 5, true, now, now - HEARTBEAT));
        assertTrue(ReplicationManager.shouldReportProgress(5, 5, true, now, 0), "first heartbeat after the sync");
    }

    @Test
    public void noHeartbeatBeforeTheInitialSyncIsComplete() {
        long now = 100_000;
        // A brand-new secondary: nothing applied, nothing reported, never attempted.
        assertFalse(ReplicationManager.shouldReportProgress(0, 0, false, now, 0),
                "a node in its initial sync must not report sequence 0 as a heartbeat");
        assertFalse(ReplicationManager.shouldReportProgress(7, 7, false, now, now - 10 * HEARTBEAT));
    }
}
