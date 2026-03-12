package de.caluga.test.morphium.driver.pool;

import de.caluga.morphium.driver.wire.PooledDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PooledDriver replica-set detection in connect().
 *
 * <p>Verifies that a non-empty {@code replSet} parameter causes the driver
 * to enter replica-set mode even when only a single seed host is configured
 * (single-node replica set scenario, e.g. Testcontainers MongoDBContainer).
 *
 * <p>These tests do NOT require a running MongoDB — they only exercise the
 * flag-setting logic at the start of {@code connect()}.
 */
@Tag("core")
@DisplayName("PooledDriver – single-node replica set detection")
class PooledDriverReplicaSetDetectionTest {

    @Test
    @DisplayName("connect(replSetName) sets replicaSet=true for single seed host")
    void singleHost_withReplSetName_isReplicaSet() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("localhost:29999"); // non-existent, won't actually connect
        driver.setServerSelectionTimeout(100);

        try {
            driver.connect("my-replica-set");
        } catch (Exception ignored) {
            // Connection to non-existent host will fail — we only care about the flag
        }

        assertTrue(driver.isReplicaSet(),
                "Driver must be in replica-set mode when replSet name is provided");
        assertEquals("my-replica-set", driver.getReplicaSetName(),
                "Driver must store the replica set name from connect()");

        driver.close();
    }

    @Test
    @DisplayName("connect(null) sets replicaSet=false for single seed host")
    void singleHost_withoutReplSetName_isNotReplicaSet() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("localhost:29999");
        driver.setServerSelectionTimeout(100);

        try {
            driver.connect(null);
        } catch (Exception ignored) {
        }

        assertFalse(driver.isReplicaSet(),
                "Driver must NOT be in replica-set mode without replSet name");

        driver.close();
    }

    @Test
    @DisplayName("connect(\"\") sets replicaSet=false for single seed host")
    void singleHost_withEmptyReplSetName_isNotReplicaSet() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("localhost:29999");
        driver.setServerSelectionTimeout(100);

        try {
            driver.connect("");
        } catch (Exception ignored) {
        }

        assertFalse(driver.isReplicaSet(),
                "Driver must NOT be in replica-set mode with empty replSet name");

        driver.close();
    }

    @Test
    @DisplayName("connect(null) sets replicaSet=true for multiple seed hosts")
    void multipleHosts_withoutReplSetName_isReplicaSet() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("localhost:29999", "localhost:29998");
        driver.setServerSelectionTimeout(100);

        try {
            driver.connect(null);
        } catch (Exception ignored) {
        }

        assertTrue(driver.isReplicaSet(),
                "Driver must be in replica-set mode when multiple hosts are configured");

        driver.close();
    }
}
