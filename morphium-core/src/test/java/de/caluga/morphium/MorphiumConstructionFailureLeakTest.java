package de.caluga.morphium;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.wire.PooledDriver;

/**
 * IM-951: when {@code connect()} times out waiting for primary discovery, the driver it started
 * (heartbeat + connection waiter) used to survive the driver. Because it is the Morphium
 * <em>constructor</em> that fails, the caller never receives an instance it could close(), so
 * those threads ran for the rest of the JVM - the "zombie JVM" thread leak measured during the
 * 2026-09-08 bus outage. Repeated in a retry loop (IM-931), the count grew without bound: ~6
 * threads per failed attempt.
 *
 * <p>{@link Morphium#setConfig(MorphiumConfig)} now runs the normal close path when construction
 * fails after the driver exists, so a failed {@code new Morphium(config)} is cleaned up here.
 * {@code PooledDriver.close()} joins the ConnectionWaiter but lets the one-shot HeartbeatCheck-
 * and ConnectionCreator- threads drain on their own, so the count is checked with a deadline
 * (#406): what matters is that it comes back to the baseline, not that it is there the instant
 * the constructor rethrows - that assertion raced the asynchronous teardown and flaked under load.
 */
@Tag("driver")
public class MorphiumConstructionFailureLeakTest {

    private static final long SETTLE_DEADLINE_MS = 10_000;

    private static String deadHost() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return "127.0.0.1:" + s.getLocalPort();
        } // closed immediately - a connect attempt is refused fast, no real Mongo needed
    }

    @Test
    @Timeout(120)
    public void failedConstructionDoesNotLeakDriverThreads() throws Exception {
        long before = driverThreadsAlive();

        for (int i = 0; i < 5; i++) {
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.driverSettings().setDriverName(PooledDriver.driverName);
            // Two seeds => replica set => connect() waits for a primary and then throws.
            cfg.clusterSettings().setHostSeed(java.util.List.of(deadHost(), deadHost()));
            cfg.driverSettings().setServerSelectionTimeout(300);
            cfg.connectionSettings().setMinConnections(5);

            // Mirrors the real failure mode: one new Morphium per retry attempt (IM-931).
            assertThrows(RuntimeException.class, () -> new Morphium(cfg));
        }

        // Every driver thread counts, daemon or not: a leaked heartbeat keeps connecting to dead
        // hosts and holding memory whether or not it could pin the JVM. The last attempt's
        // HeartbeatCheck thread may still be logging its connect failure when the constructor
        // rethrows, so wait for the count to settle instead of sampling it synchronously.
        long deadline = System.currentTimeMillis() + SETTLE_DEADLINE_MS;
        long alive = driverThreadsAlive();
        while (alive > before && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            alive = driverThreadsAlive();
        }

        assertTrue(alive <= before,
            "each failed new Morphium(config) must release its driver - otherwise the heartbeat/"
                + "pool threads accumulate for the life of the JVM (IM-951); still alive after "
                + SETTLE_DEADLINE_MS + " ms: " + alive + ", baseline " + before);
    }

    /**
     * Counts live PooledDriver threads of any daemon status. Name-based, but keyed by Thread
     * object (not a Set of names) - a Set collapses the duplicate MCon- entries and hides the
     * growth, the exact mistake that produced a false "no accumulation" reading during the IM-931
     * investigation.
     */
    private static long driverThreadsAlive() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .filter(t -> t.getName().startsWith("MCon-")
                || t.getName().startsWith("ConnectionWaiter")
                || t.getName().startsWith("ConnectionCreator-")
                || t.getName().startsWith("HeartbeatCheck-"))
            .count();
    }
}
