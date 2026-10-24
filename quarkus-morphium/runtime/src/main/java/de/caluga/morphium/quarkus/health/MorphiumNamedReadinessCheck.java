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
import de.caluga.morphium.quarkus.NamedMorphiumRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness of the named Morphium instances (MOR-032): UP if every configured instance is connected, DOWN if one is not. One entry per instance, named after it. With no named
 * instance the check is UP. The default instance has its own check, {@link MorphiumReadinessCheck}.
 */
@Readiness
@ApplicationScoped
public class MorphiumNamedReadinessCheck implements HealthCheck {

    @Inject
    NamedMorphiumRegistry registry;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Morphium named instances readiness check");
        boolean allUp = true;

        for (String name : registry.names()) {
            try {
                Morphium morphium = registry.get(name);
                boolean connected = morphium.getDriver().isConnected();
                builder.withData(name, connected ? "UP (" + morphium.getConfig().connectionSettings().getDatabase() + ")" : "DOWN");
                allUp &= connected;
            } catch (Exception e) {
                builder.withData(name, "DOWN: " + e.getMessage());
                allUp = false;
            }
        }

        return builder.status(allUp).build();
    }
}
