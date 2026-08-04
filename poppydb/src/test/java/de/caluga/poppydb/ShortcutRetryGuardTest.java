package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Seam test for the initial-sync shortcut-retry guard (task-4 follow-up item A).
 *
 * <p>The initial-sync retry loop in {@code startInitialSyncOnce()} tries the dbHash consistency
 * shortcut first and, on a miss, falls back to {@code clearLocalDatabases()} +
 * {@code performInitialSync()}. If that attempt is then discarded because the watch died or
 * re-registered mid-copy ({@code watchInvalidatedDuringSnapshot}), the loop retries. Without a
 * guard, the retry would call the shortcut again - and since local data now holds this very
 * cycle's own (discarded) full copy, the dbHash comparison would very plausibly match the primary,
 * misreporting a full resync as a shortcut ({@code lastSyncWasShortcut(true)}) even though a full
 * clear + copy actually happened. {@link ReplicationManager#shouldAttemptConsistencyShortcut()} is
 * the guard that prevents this: it goes false the moment {@code clearLocalDatabases()} runs, for
 * the rest of that sync cycle. This test exercises that guard directly through package-private
 * access, the same seam pattern {@link WatchGenerationTest} uses for the watch-generation guard.
 */
public class ShortcutRetryGuardTest {

    @Test
    public void shortcutIsAttemptedBeforeAnyWipeThisCycle() {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);

        assertTrue(mgr.shouldAttemptConsistencyShortcut(),
                "a fresh sync cycle that has not wiped local data yet must still consider the shortcut");
    }

    @Test
    public void shortcutIsSkippedOnRetryAfterAWipeThisCycle() {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", 12345);

        // Simulates clearLocalDatabases() having run once already in this sync cycle (the
        // attempt that was then discarded by the watchInvalidatedDuringSnapshot guard).
        mgr.setWipedThisSyncCycleForTest(true);

        assertFalse(mgr.shouldAttemptConsistencyShortcut(),
                "a retry within a cycle that already wiped local data must not re-attempt the "
                        + "shortcut - it would be comparing the primary against data this very "
                        + "cycle just copied, misreporting a full sync as a shortcut");
    }
}
