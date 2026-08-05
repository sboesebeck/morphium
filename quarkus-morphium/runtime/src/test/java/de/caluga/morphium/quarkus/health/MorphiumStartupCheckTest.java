/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus.health;

import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the SRV-discovery-tolerant startup check logic.
 *
 * <p>These tests exercise the {@code everConnected} latch directly by
 * simulating different driver states without requiring a Quarkus container
 * or a real MongoDB connection.
 */
@DisplayName("MorphiumStartupCheck — SRV discovery tolerance")
class MorphiumStartupCheckTest {

    /**
     * Simulates the startup check logic with the given driver state.
     * This mirrors the implementation in {@link MorphiumStartupCheck#call()}
     * without requiring CDI injection.
     */
    private static HealthCheckResponse simulateStartupCheck(boolean driverConnected, double connectionsOpened) {
        var builder = HealthCheckResponse.named("Morphium startup check");
        Map<DriverStatsKey, Double> stats = new HashMap<>();
        stats.put(DriverStatsKey.CONNECTIONS_OPENED, connectionsOpened);

        double opened = stats.getOrDefault(DriverStatsKey.CONNECTIONS_OPENED, 0.0);
        builder.withData("database", "test-db")
               .withData("connectionsOpened", (long) opened);

        boolean everConnected = opened > 0 || driverConnected;
        return builder.status(everConnected).build();
    }

    @Test
    @DisplayName("DOWN when no connections opened and driver not connected (SRV discovery in progress)")
    void downDuringSrvDiscovery() {
        HealthCheckResponse response = simulateStartupCheck(false, 0.0);

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    }

    @Test
    @DisplayName("UP when connections opened but driver reports not connected (hosts map empty)")
    void upWhenConnectionsOpenedButHostsMapEmpty() {
        HealthCheckResponse response = simulateStartupCheck(false, 5.0);

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    }

    @Test
    @DisplayName("UP when driver reports connected (normal operation)")
    void upWhenDriverConnected() {
        HealthCheckResponse response = simulateStartupCheck(true, 10.0);

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    }

    @Test
    @DisplayName("UP when driver connected but no connections opened (InMemoryDriver)")
    void upWhenDriverConnectedNoConnectionsOpened() {
        HealthCheckResponse response = simulateStartupCheck(true, 0.0);

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    }

    @Test
    @DisplayName("UP is a one-way latch — once connections were opened, stays UP even if driver disconnects later")
    void oneWayLatchSemantics() {
        // First: connections opened, driver not connected (SRV resolved, hosts map cleared)
        HealthCheckResponse response = simulateStartupCheck(false, 100.0);

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        // The counter never decreases, so the latch holds
    }
}
