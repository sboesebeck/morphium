package de.caluga.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriverException;

/**
 * When connect() times out waiting for primary discovery on an unreachable replica set, the
 * resulting exception must surface the underlying connection error (e.g. connection refused, a
 * TLS handshake failure) instead of the bare "No primary node found" message - otherwise the
 * real cause is only visible in a DEBUG/heartbeat log line that most setups never see.
 */
public class PooledDriverConnectErrorTest {

    @Test
    public void timeoutExceptionCarriesLastConnectionError() {
        PooledDriver drv = new PooledDriver();
        // Port 1 is a privileged, essentially always-refused port - fails fast without needing a
        // real unreachable host (which would depend on network/firewall behavior in CI).
        drv.setHostSeed("localhost:1", "localhost:2", "localhost:3");
        drv.setServerSelectionTimeout(300);

        try {
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> drv.connect(null));

            assertTrue(ex.getMessage().contains("No primary node found"),
                "should keep the original timeout message: " + ex.getMessage());
            assertNotNull(ex.getCause(),
                "should chain the last connection failure as the cause instead of leaving it undiagnosable");
        } finally {
            // connect() starts the heartbeat thread before the primary-discovery timeout fires -
            // without closing, it keeps hammering localhost:1/2/3 for the rest of the surefire JVM.
            drv.close();
        }
    }
}
