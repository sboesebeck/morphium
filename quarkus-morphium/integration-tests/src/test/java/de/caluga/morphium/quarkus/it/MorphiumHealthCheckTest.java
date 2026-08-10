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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.quarkus.testing.InMemMorphiumTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * Verifies that Morphium health checks are registered and report UP
 * when connected via the InMemDriver.
 */
@QuarkusTest
@TestProfile(InMemMorphiumTestProfile.class)
@DisplayName("Morphium health checks (enabled)")
class MorphiumHealthCheckTest {

    @Test
    @DisplayName("GET /q/health/live -> Morphium liveness check UP")
    void livenessCheckIsUp() {
        given()
            .when().get("/q/health/live")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium liveness check"))
                .body("checks.find { it.name == 'Morphium liveness check' }.status", is("UP"));
    }

    @Test
    @DisplayName("GET /q/health/ready -> Morphium readiness check UP")
    void readinessCheckIsUp() {
        given()
            .when().get("/q/health/ready")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium readiness check"))
                .body("checks.find { it.name == 'Morphium readiness check' }.status", is("UP"));
    }

    @Test
    @DisplayName("GET /q/health/started -> Morphium startup check UP")
    void startupCheckIsUp() {
        given()
            .when().get("/q/health/started")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium startup check"))
                .body("checks.find { it.name == 'Morphium startup check' }.status", is("UP"));
    }

    @Test
    @DisplayName("GET /q/health -> all checks contain database metadata")
    void healthChecksContainDatabaseMetadata() {
        given()
            .when().get("/q/health")
            .then()
                .statusCode(200)
                .body("checks.name", hasItems(
                        "Morphium liveness check",
                        "Morphium readiness check",
                        "Morphium startup check"))
                .body("checks.find { it.name == 'Morphium liveness check' }.data.database", is("inmem-test"))
                .body("checks.find { it.name == 'Morphium readiness check' }.data.database", is("inmem-test"))
                .body("checks.find { it.name == 'Morphium startup check' }.data.database", is("inmem-test"));
    }
}
