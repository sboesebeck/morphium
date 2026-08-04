package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

/**
 * Seam test for the abort-must-never-wipe hardening (P1): a {@code stop()}'d
 * {@link ReplicationManager} must never let its (about to die) initial-sync thread proceed into
 * {@code clearLocalDatabases()} - wiping local data that an already-running replacement RM
 * (PoppyDB replaces RMs on leader change) may be populating in parallel.
 *
 * <p>Bug chain fixed here: {@code stop()} used to interrupt {@code initialSyncThread} without
 * joining it. {@code tryConsistencyShortcut()}'s {@code InterruptedException} handler correctly
 * restores the interrupt flag, but on its own that changes nothing - the retry loop had no check
 * between the shortcut attempt and {@code clearLocalDatabases()}, so a cycle that {@code stop()}
 * had already abandoned (running == false) would still wipe local data before dying.
 *
 * <p><b>Why a deterministic pause seam, not real network timing:</b> a first attempt at this test
 * pointed the RM at a genuinely unreachable (nothing-listening) port and raced {@code stop()}
 * against real driver calls. That turned out to give no usable window at all: an unreachable port
 * fails via "No such host" in single-digit milliseconds with no interruptible retry/sleep in the
 * path, so by the time the test could observe "the shortcut attempt started" and call
 * {@code stop()}, the (legitimate, pre-stop) full-sync fallback had already run
 * {@code clearLocalDatabases()} once - a false failure unrelated to {@code stop()} at all.
 * {@link ReplicationManager#armTestPauseInShortcutForTest()} instead blocks the sync thread
 * INSIDE {@code tryConsistencyShortcut()}, at the exact spot a real blocking primary-side call
 * would sit, so the test can reproduce "stop() interrupts a still-attempting shortcut" on demand
 * instead of gambling on timing.
 */
public class AbortNeverWipesTest {

    /**
     * A free, currently-unbound port. Nothing needs to actually listen on it - the pause seam
     * blocks the sync thread before it ever touches the primary driver - but a real (bound then
     * released) port avoids any chance of colliding with something else running on the test host.
     */
    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** Poll a condition with a generous timeout - never a fixed sleep for an async condition. */
    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    @Test
    public void stopDuringSyncCycleNeverWipesLocalData() throws Exception {
        ReplicationManager mgr = new ReplicationManager(null, "localhost", freePort());

        try {
            mgr.armTestPauseInShortcutForTest();
            mgr.start();
            // Skip the watchLive wait so the retry loop goes straight into the consistency
            // shortcut attempt, where it blocks on the armed test pause.
            mgr.setWatchLiveForTest(true);

            assertTrue(poll(5000, () -> mgr.getConsistencyShortcutAttemptsForTest() >= 1),
                    "the sync thread must have entered (and be blocked inside) "
                            + "tryConsistencyShortcut()");
            assertEquals(0, mgr.getClearLocalDatabasesInvocationsForTest(),
                    "sanity: nothing has wiped local data yet - the thread is still paused "
                            + "before ever reaching the shortcut's own driver calls");

            Thread syncThread = mgr.getInitialSyncThreadForTest();
            assertNotNull(syncThread, "sanity: the initial-sync thread must be running");

            // The race: stop() while the thread is blocked mid-shortcut-attempt. Its interrupt()
            // throws InterruptedException out of the test pause's latch.await(), landing exactly
            // in tryConsistencyShortcut()'s InterruptedException handler - the same path a real,
            // still-in-flight primary-side call would take.
            mgr.stop();

            // stop() must have joined the thread before returning (bounded 5s) - it must be dead
            // by now, not still racing a replacement RM in the background.
            assertFalse(syncThread.isAlive(),
                    "the initial-sync thread must be dead once stop() returns");

            // The crux: a cycle stop() abandoned mid-shortcut must never have wiped local data.
            assertEquals(0, mgr.getClearLocalDatabasesInvocationsForTest(),
                    "a sync cycle that stop() interrupted must never call clearLocalDatabases()");
        } finally {
            mgr.stop(); // idempotent; harmless if already stopped
        }
    }
}
