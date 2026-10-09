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
 * (heartbeat + connection waiter) used to survive the driver as <em>non-daemon</em> threads.
 * Because it is the Morphium <em>constructor</em> that fails, the caller never receives an instance
 * it could close(), so those threads ran for the rest of the JVM - the "zombie JVM" thread leak
 * measured during the 2026-09-08 bus outage. Repeated in a retry loop (IM-931), the count grew
 * without bound: ~6 threads per failed attempt.
 *
 * <p>{@link Morphium#setConfig(MorphiumConfig)} now runs the normal close path when construction
 * fails after the driver exists, so a failed {@code new Morphium(config)} is cleaned up here. The
 * driver's worker threads (ConnectionWaiter, ConnectionCreator-*, HeartbeatCheck-*, and the MCon-
 * executor factory) are daemon and {@code close()} joins the ConnectionWaiter, so no non-daemon
 * driver thread can outlive a failed construction - the property this test asserts.
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
        long before = nonDaemonDriverThreadsAlive();

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

        // Only non-daemon driver threads can pin the JVM after a failed construction (the zombie
        // the IM-951 fixes exist for). The daemon workers may still be draining when the close path
        // returns, but a daemon thread cannot keep the JVM alive - so we assert no non-daemon ones
        // accumulate, which is stable under load and does not flake on short-lived teardown.
        assertTrue(nonDaemonDriverThreadsAlive() <= before,
            "each failed new Morphium(config) must release its driver - otherwise non-daemon heartbeat/"
                + "pool threads accumulate for the life of the JVM (IM-951)");
    }

    /**
     * Counts live <em>non-daemon</em> PooledDriver threads. Name-based, but keyed by Thread object
     * (not a Set of names) - a Set collapses the duplicate MCon- entries and hides the growth, the
     * exact mistake that produced a false "no accumulation" reading during the IM-931 investigation.
     * Convention: the driver's helper threads are daemon, so only a leaked non-daemon one is a
     * zombie-JVM risk and worth counting here.
     */
    private static long nonDaemonDriverThreadsAlive() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .filter(t -> !t.isDaemon())
            .filter(t -> t.getName().startsWith("MCon-")
                || t.getName().startsWith("ConnectionWaiter")
                || t.getName().startsWith("ConnectionCreator-")
                || t.getName().startsWith("HeartbeatCheck-"))
            .count();
    }
}
