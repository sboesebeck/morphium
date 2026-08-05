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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import java.util.Map;

/**
 * Readiness health check for Morphium.
 *
 * <p>Reports DOWN only when the driver is no longer connected. Pool statistics
 * (connections in use, threads waiting, etc.) are included as informational
 * metadata but do <em>not</em> affect the UP/DOWN status.
 *
 * <p>Rationale: transient pool saturation during bulk operations is normal and
 * should not cause Kubernetes to remove the pod from service. Pool utilization
 * belongs in metrics/monitoring (e.g. Prometheus), not in readiness probes.
 * This is consistent with how other Quarkus extensions handle readiness
 * (e.g. the MongoDB client extension only pings the server).
 */
@Readiness
@ApplicationScoped
public class MorphiumReadinessCheck implements HealthCheck {

    @Inject
    Morphium morphium;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Morphium readiness check");
        try {
            MorphiumDriver driver = morphium.getDriver();
            boolean connected = driver.isConnected();

            builder.withData("database", morphium.getConfig().connectionSettings().getDatabase())
                   .status(connected);

            // Pool stats are best-effort informational metadata.
            // During heavy load (e.g. bulk imports), stat collection may fail --
            // this must never affect the UP/DOWN status.
            try {
                Map<DriverStatsKey, Double> stats = driver.getDriverStats();
                long borrowed = toLong(stats, DriverStatsKey.CONNECTIONS_BORROWED);
                long released = toLong(stats, DriverStatsKey.CONNECTIONS_RELEASED);
                builder.withData("connectionsInUse", toLong(stats, DriverStatsKey.CONNECTIONS_IN_USE))
                       .withData("connectionsInPool", toLong(stats, DriverStatsKey.CONNECTIONS_IN_POOL))
                       .withData("connectionsBorrowed", borrowed)
                       .withData("connectionsReleased", released)
                       .withData("connectionsBorrowedMinusReleased", borrowed - released)
                       .withData("threadsWaiting", toLong(stats, DriverStatsKey.THREADS_WAITING_FOR_CONNECTION))
                       .withData("errors", toLong(stats, DriverStatsKey.ERRORS));

                Map<String, Integer> hostConnections = driver.getNumConnectionsByHost();
                if (hostConnections != null && !hostConnections.isEmpty()) {
                    for (Map.Entry<String, Integer> entry : hostConnections.entrySet()) {
                        builder.withData("host:" + entry.getKey(), entry.getValue());
                    }
                }
            } catch (Exception statsEx) {
                builder.withData("statsUnavailable", statsEx.getMessage());
            }

            return builder.build();
        } catch (Exception e) {
            return builder.down().withData("error", e.getMessage()).build();
        }
    }

    private static long toLong(Map<DriverStatsKey, Double> stats, DriverStatsKey key) {
        return stats.getOrDefault(key, 0.0).longValue();
    }
}
