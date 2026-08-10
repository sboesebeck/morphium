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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasItem;

/**
 * Verifies that Morphium health checks are absent when
 * {@code quarkus.morphium.health.enabled=false}.
 */
@QuarkusTest
@TestProfile(MorphiumHealthCheckDisabledTest.DisabledHealthProfile.class)
@DisplayName("Morphium health checks (disabled)")
class MorphiumHealthCheckDisabledTest {

    /**
     * Test profile that disables Morphium health checks via build-time config.
     */
    public static class DisabledHealthProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.morphium.driver-name", "InMemDriver",
                    "quarkus.morphium.database", "inmem-test",
                    "quarkus.morphium.devservices.enabled", "false",
                    "quarkus.morphium.health.enabled", "false"
            );
        }
    }

    @Test
    @DisplayName("GET /q/health -> no Morphium checks present")
    void noMorphiumChecksWhenDisabled() {
        given()
            .when().get("/q/health")
            .then()
                .statusCode(200)
                .body("checks.name", not(hasItem("Morphium liveness check")))
                .body("checks.name", not(hasItem("Morphium readiness check")))
                .body("checks.name", not(hasItem("Morphium startup check")));
    }
}
