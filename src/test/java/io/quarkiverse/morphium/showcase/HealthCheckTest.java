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
package io.quarkiverse.morphium.showcase;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests verifying that Morphium health checks are registered and UP.
 *
 * <p>The {@code quarkus-morphium} extension automatically registers liveness, readiness
 * and startup health checks when {@code quarkus-smallrye-health} is on the classpath.
 * These tests verify the JSON endpoints return the expected check names and status.</p>
 */
@QuarkusTest
class HealthCheckTest {

    @Test
    void livenessEndpointContainsMorphiumCheck() {
        given()
            .when().get("/q/health/live")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium liveness check"));
    }

    @Test
    void readinessEndpointContainsMorphiumCheck() {
        given()
            .when().get("/q/health/ready")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium readiness check"));
    }

    @Test
    void startupEndpointContainsMorphiumCheck() {
        given()
            .when().get("/q/health/started")
            .then()
                .statusCode(200)
                .body("status", is("UP"))
                .body("checks.name", hasItem("Morphium startup check"));
    }

    @Test
    void healthChecksContainDatabaseMetadata() {
        given()
            .when().get("/q/health")
            .then()
                .statusCode(200)
                .body("checks.find { it.name == 'Morphium liveness check' }.data.database", is("inmem-test"))
                .body("checks.find { it.name == 'Morphium readiness check' }.data.database", is("inmem-test"))
                .body("checks.find { it.name == 'Morphium startup check' }.data.database", is("inmem-test"));
    }

    @Test
    void healthShowcasePageRendersSuccessfully() {
        given()
            .when().get("/health")
            .then()
                .statusCode(200)
                .contentType("text/html")
                .body(containsString("MicroProfile Health"))
                .body(containsString("Liveness Check"))
                .body(containsString("Readiness Check"))
                .body(containsString("Startup Check"));
    }
}
