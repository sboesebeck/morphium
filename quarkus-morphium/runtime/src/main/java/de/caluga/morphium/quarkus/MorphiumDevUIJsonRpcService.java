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
package de.caluga.morphium.quarkus;

import de.caluga.morphium.Morphium;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JsonRPC service for the Quarkus Dev UI.
 *
 * <p>Provides runtime connection information by querying the actual {@link Morphium}
 * instance, including the real replica set status detected via the MongoDB hello handshake.
 */
@Singleton
public class MorphiumDevUIJsonRpcService {

    private static final Logger log = Logger.getLogger(MorphiumDevUIJsonRpcService.class);

    @Inject
    Morphium morphium;

    public List<Map<String, String>> getConnectionInfo() {
        List<Map<String, String>> rows = new ArrayList<>();
        try {
            var config = morphium.getConfig();
            var driver = morphium.getDriver();

            var clusterSettings = config.clusterSettings();
            var hostSeed = clusterSettings.getHostSeed();
            String hosts;
            if (hostSeed != null && !hostSeed.isEmpty()) {
                hosts = String.join(", ", hostSeed);
            } else {
                String atlasUrl = clusterSettings.getAtlasUrl();
                hosts = (atlasUrl != null && !atlasUrl.isBlank()) ? sanitizeUri(atlasUrl) : "unknown";
            }
            String database = config.connectionSettings().getDatabase();
            boolean isReplicaSet = driver.isReplicaSet();
            String mode = isReplicaSet ? "Replica Set (transactions enabled)" : "Standalone";
            String driverName = driver.getClass().getSimpleName();
            String status = (driver.isConnected()) ? "Connected" : "Disconnected";

            rows.add(row("Hosts", hosts));
            rows.add(row("Database", database));
            rows.add(row("Mode", mode));
            rows.add(row("Driver", driverName));
            rows.add(row("Status", status));
        } catch (Exception e) {
            log.warn("Failed to retrieve Morphium connection info for Dev UI", e);
            rows.add(row("Status", "Error retrieving connection info"));
        }
        return rows;
    }

    /**
     * Strips userinfo (credentials) from a MongoDB URI to prevent exposing
     * passwords in the Dev UI. For example, {@code mongodb+srv://user:pass@host}
     * becomes {@code mongodb+srv://***@host}.
     */
    private static String sanitizeUri(String uri) {
        // Pattern: scheme://userinfo@host... → scheme://***@host...
        return uri.replaceFirst("(mongodb(?:\\+srv)?://)([^@]+)@", "$1***@");
    }

    private static Map<String, String> row(String property, String value) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("Property", property);
        map.put("Value", value);
        return map;
    }
}
