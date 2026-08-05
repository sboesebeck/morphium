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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Liveness;

/**
 * Liveness health check for Morphium.
 *
 * <p>Reports DOWN when the Morphium driver is no longer connected.
 * A DOWN liveness probe causes Kubernetes to restart the pod.
 */
@Liveness
@ApplicationScoped
public class MorphiumLivenessCheck implements HealthCheck {

    @Inject
    Morphium morphium;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Morphium liveness check");
        try {
            boolean connected = morphium.getDriver().isConnected();
            builder.withData("database", morphium.getConfig().connectionSettings().getDatabase())
                   .withData("driver", morphium.getDriver().getClass().getSimpleName());
            return builder.status(connected).build();
        } catch (Exception e) {
            return builder.down().withData("error", e.getMessage()).build();
        }
    }
}
