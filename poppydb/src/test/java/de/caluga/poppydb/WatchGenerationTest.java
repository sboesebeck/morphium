package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Seam test for the initial-sync watch-generation guard.
 *
 * <p>The initial-sync snapshot waits for the change-stream watch to be live before it starts
 * copying, but the watch can still die and be re-established (a fresh watch that starts "from
 * now" with no resume point, because initial sync has not completed yet) WHILE the copy is in
 * flight. Any write between the old watch's death and the new watch's registration is then lost.
 *
 * <p>To close that window the snapshot captures the watch generation before the copy and, when
 * the copy finishes, redoes it if the generation changed or the watch is no longer live. This
 * test exercises that detection predicate directly through package-private access, which is the
 * seam the production loop uses.
 */
public class WatchGenerationTest {

    @Test
    public void detectsWatchDeathAndReopenDuringSnapshot() {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);

        // Watch becomes live; snapshot captures the current generation before copying.
        mgr.bumpWatchGenerationForTest();      // simulates the registration callback firing
        mgr.setWatchLiveForTest(true);
        long captured = mgr.watchGeneration.get();

        // Healthy watch throughout the copy: not invalidated.
        assertFalse(mgr.watchInvalidatedDuringSnapshot(captured),
                "a watch that stays live at the same generation must not invalidate the snapshot");

        // Watch dies and a NEW watch re-establishes mid-copy (generation bumps): invalidated.
        mgr.bumpWatchGenerationForTest();
        assertTrue(mgr.watchInvalidatedDuringSnapshot(captured),
                "a watch generation change during the copy must invalidate the snapshot");
    }

    @Test
    public void detectsWatchDeathWithoutReopenDuringSnapshot() {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);
        mgr.bumpWatchGenerationForTest();
        mgr.setWatchLiveForTest(true);
        long captured = mgr.watchGeneration.get();

        // Watch dies and no new watch has re-registered yet (generation unchanged, watchLive false):
        // still invalidated, because the copy raced a dead watch.
        mgr.setWatchLiveForTest(false);
        assertTrue(mgr.watchInvalidatedDuringSnapshot(captured),
                "a watch that is no longer live must invalidate the snapshot");
    }
}
