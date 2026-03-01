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
package io.quarkiverse.morphium.showcase.health;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JAX-RS resource demonstrating MicroProfile Health integration with Morphium.
 *
 * <p>The {@code quarkus-morphium} extension automatically registers three health checks
 * when {@code quarkus-smallrye-health} is on the classpath:</p>
 * <ul>
 *   <li><strong>Liveness</strong> ({@code /q/health/live}) -- Is the driver connected?</li>
 *   <li><strong>Readiness</strong> ({@code /q/health/ready}) -- Is the connection pool healthy?</li>
 *   <li><strong>Startup</strong> ({@code /q/health/started}) -- Was the initial connection established?</li>
 * </ul>
 *
 * <p>This showcase page displays the underlying Morphium driver statistics that the health
 * checks use, alongside links to the standard MicroProfile Health JSON endpoints.</p>
 *
 * <h3>Morphium Driver APIs Used</h3>
 * <pre>{@code
 * morphium.getDriver().isConnected()          // Liveness
 * morphium.getDriver().getDriverStats()       // Pool statistics
 * morphium.getDriver().getNumConnectionsByHost()  // Per-host connections
 * }</pre>
 *
 * @see de.caluga.morphium.driver.MorphiumDriver#isConnected()
 * @see de.caluga.morphium.driver.MorphiumDriver#getDriverStats()
 */
@Path("/health")
public class HealthResource {

    @Inject
    Template health;

    @Inject
    Morphium morphium;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "MorphiumDriver, Connection Pool, Health Checks"),
            new DocLink("/docs/api-reference", "API Reference", "isConnected(), getDriverStats(), getNumConnectionsByHost()")
    );

    /**
     * Renders the health dashboard page with live Morphium driver statistics.
     *
     * <p>Collects the same data that the MicroProfile Health checks evaluate:
     * connection status, pool statistics, and per-host connection counts.</p>
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        MorphiumDriver driver = morphium.getDriver();
        boolean connected = driver.isConnected();
        String driverName = driver.getClass().getSimpleName();
        String database = morphium.getConfig().connectionSettings().getDatabase();

        // Pool statistics (used by readiness and startup checks)
        Map<DriverStatsKey, Double> stats = driver.getDriverStats();
        Map<String, Long> poolStats = new LinkedHashMap<>();
        poolStats.put("Connections In Pool", toLong(stats, DriverStatsKey.CONNECTIONS_IN_POOL));
        poolStats.put("Connections In Use", toLong(stats, DriverStatsKey.CONNECTIONS_IN_USE));
        poolStats.put("Connections Opened", toLong(stats, DriverStatsKey.CONNECTIONS_OPENED));
        poolStats.put("Connections Closed", toLong(stats, DriverStatsKey.CONNECTIONS_CLOSED));
        poolStats.put("Connections Borrowed", toLong(stats, DriverStatsKey.CONNECTIONS_BORROWED));
        poolStats.put("Connections Released", toLong(stats, DriverStatsKey.CONNECTIONS_RELEASED));
        poolStats.put("Threads Waiting", toLong(stats, DriverStatsKey.THREADS_WAITING_FOR_CONNECTION));
        poolStats.put("Threads Created", toLong(stats, DriverStatsKey.THREADS_CREATED));
        poolStats.put("Errors", toLong(stats, DriverStatsKey.ERRORS));
        poolStats.put("Failovers", toLong(stats, DriverStatsKey.FAILOVERS));
        poolStats.put("Messages Sent", toLong(stats, DriverStatsKey.MSG_SENT));
        poolStats.put("Replies Received", toLong(stats, DriverStatsKey.REPLY_RECEIVED));
        poolStats.put("Replies Processed", toLong(stats, DriverStatsKey.REPLY_PROCESSED));
        poolStats.put("Replies In Memory", toLong(stats, DriverStatsKey.REPLY_IN_MEM));

        // Per-host connections (used by readiness check)
        Map<String, Integer> hostConnections = driver.getNumConnectionsByHost();
        if (hostConnections == null) {
            hostConnections = Map.of();
        }

        // Derive health status (same logic as the health check classes)
        // All three checks only depend on isConnected() -- pool stats are informational only
        String livenessStatus = connected ? "UP" : "DOWN";
        String readinessStatus = connected ? "UP" : "DOWN";
        String startupStatus = connected ? "UP" : "DOWN";

        return health.data("active", "health")
                .data("connected", connected)
                .data("driverName", driverName)
                .data("database", database)
                .data("poolStats", poolStats)
                .data("hostConnections", hostConnections)
                .data("livenessStatus", livenessStatus)
                .data("readinessStatus", readinessStatus)
                .data("startupStatus", startupStatus)
                .data("docLinks", DOC_LINKS);
    }

    private static long toLong(Map<DriverStatsKey, Double> stats, DriverStatsKey key) {
        return stats.getOrDefault(key, 0.0).longValue();
    }
}
