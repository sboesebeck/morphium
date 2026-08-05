package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.ServerSocket;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.wire.PooledDriver;

/**
 * {@link PooledDriver#getLastConnectFailure()} - no real MongoDB needed, unlike
 * {@link PooledDriverTest} (which requires one via {@code @Tag("external")}): a closed local
 * port is a perfectly good stand-in for "seed host unreachable", which is exactly the case this
 * accessor exists for (surfacing a swallowed connect failure - e.g. a TLS handshake error - to a
 * caller that only sees {@code isConnected() == false} otherwise, see Morpheus's
 * MorphiumConnectionFactory).
 */
public class PooledDriverConnectFailureTest {

    @Test
    public void freshDriverHasNoRecordedFailure() {
        PooledDriver drv = new PooledDriver();
        assertNull(drv.getLastConnectFailure(), "a driver that never attempted a connection has nothing to report");
    }

    @Test
    public void unreachableSeedIsRecordedAsLastConnectFailure() throws Exception {
        int deadPort;
        try (ServerSocket s = new ServerSocket(0)) {
            deadPort = s.getLocalPort();
        } // closed immediately - nothing listens here now, so a connect attempt is refused fast

        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("localhost:" + deadPort);

        try {
            // Non-replicaset (single seed): connect() does not itself throw - it tolerates the
            // failed seed and falls back to "treat first seed as primary" (existing behavior,
            // relied upon by every other single-host caller). The failure must still be visible
            // via getLastConnectFailure() so a caller doing its own isConnected() polling (like
            // Morpheus) can report the real cause instead of a bare timeout.
            drv.connect(null);
            assertNotNull(drv.getLastConnectFailure(),
                    "a refused connection to the only seed must be recorded, even though connect() itself didn't throw");
        } finally {
            drv.close();
        }
    }
}
