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
 * (heartbeat + connection waiter, both non-daemon) would previously survive the driver. Because it
 * is the Morphium <em>constructor</em> that fails, the caller never receives an instance it could
 * close(), so those threads ran for the rest of the JVM - the "zombie JVM" thread leak measured
 * during the 2026-09-08 bus outage. Repeated in a retry loop (IM-931), the count grows without
 * bound: ~6 threads per failed attempt.
 *
 * <p>{@link Morphium#setConfig(MorphiumConfig)} now runs the normal close path when construction
 * fails after the driver exists, so a failed {@code new Morphium(config)} is cleaned up here.
 */
@Tag("driver")
public class MorphiumConstructionFailureLeakTest {

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

        // The close path runs inside the constructor's failure handling, so the threads are gone
        // by the time the constructor rethrows - no settling delay needed.
        assertTrue(driverThreadsAlive() <= before,
            "each failed new Morphium(config) must release its driver - otherwise the heartbeat/"
                + "pool threads accumulate for the life of the JVM (IM-951)");
    }

    /**
     * Counts live PooledDriver threads. Name-based, but keyed by Thread object (not a Set of
     * names) - a Set collapses the duplicate MCon- entries and hides the growth, the exact mistake
     * that produced a false "no accumulation" reading during the IM-931 investigation.
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
