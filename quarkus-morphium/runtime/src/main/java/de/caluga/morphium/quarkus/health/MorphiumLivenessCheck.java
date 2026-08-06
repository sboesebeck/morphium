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
 * <p>Reports DOWN only if the {@link Morphium} bean itself is unusable (e.g. a
 * misconfiguration prevents even constructing the driver). Does <em>not</em> report DOWN on a
 * lost MongoDB connection.
 *
 * <p>Rationale: liveness answers "is this process alive and should Kubernetes restart it if
 * not", not "is a downstream dependency reachable". Restarting the pod does not fix an
 * unreachable MongoDB server — it just adds a restart storm on top of the outage, restarting
 * every replica in the deployment simultaneously and taking the application fully offline until
 * MongoDB itself recovers. DB connectivity belongs in the readiness probe instead
 * ({@link MorphiumReadinessCheck}), which correctly takes the pod out of the Service's endpoint
 * list without killing it, and automatically re-adds it once the connection recovers.
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
            builder.withData("database", morphium.getConfig().connectionSettings().getDatabase())
                   .withData("driver", morphium.getDriver().getClass().getSimpleName());
            return builder.up().build();
        } catch (Exception e) {
            return builder.down().withData("error", e.getMessage()).build();
        }
    }
}
