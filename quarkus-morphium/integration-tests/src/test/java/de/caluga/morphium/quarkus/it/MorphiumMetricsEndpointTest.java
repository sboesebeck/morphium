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

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-031: the meters of the extension appear on the Quarkus metrics endpoint ({@code /q/metrics},
 * Prometheus format) with a real MongoDB behind them (Dev Services, a container started by
 * Testcontainers): the connection pool gauges, and the timer of the update operations after writes
 * through {@code Query.inc} and {@code Query.set}.
 *
 * <p>Skipped without a Docker daemon, like the other tests that need a real MongoDB.
 */
@QuarkusTest
@ExtendWith(DockerAvailableCondition.class)
@TestProfile(MorphiumMetricsEndpointTest.RealMongoProfile.class)
@DisplayName("Morphium meters on /q/metrics")
class MorphiumMetricsEndpointTest {

    public static class RealMongoProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.morphium.database", "metrics-test",
                    "quarkus.morphium.driver-name", "PooledDriver",
                    "quarkus.morphium.devservices.enabled", "true",
                    "quarkus.morphium.devservices.replica-set", "false"
            );
        }
    }

    @Inject
    Morphium morphium;

    private String scrape() {
        return given().when().get("/q/metrics").then().statusCode(200).extract().asString();
    }

    @Test
    @DisplayName("pool gauges and the update timer are exposed, and the timer counts the writes")
    void metricsAreExposed() {
        ItemEntity item = new ItemEntity();
        item.setName("metrics");
        item.setPrice(1.0);
        morphium.store(item);

        for (int i = 0; i < 3; i++) {
            morphium.createQueryFor(ItemEntity.class).f("name").eq("metrics").inc("price", 1);
        }
        morphium.createQueryFor(ItemEntity.class).f("name").eq("metrics").set("price", 10.0);

        String body = scrape();

        assertThat(body).contains("morphium_driver_connections_in_use{database=\"metrics-test\"");
        assertThat(body).contains("morphium_driver_connections_borrowed_total{database=\"metrics-test\"");
        assertThat(body).contains("morphium_driver_threads_waiting{database=\"metrics-test\"");

        assertThat(countOf(body, "inc")).isEqualTo(3.0);
        assertThat(countOf(body, "set")).isEqualTo(1.0);
        assertThat(body).contains("morphium_operations_duration_seconds_sum{");
    }

    private static double countOf(String body, String updateType) {
        return body.lines()
                .filter(l -> l.startsWith("morphium_operations_duration_seconds_count{"))
                .filter(l -> l.contains("update_type=\"" + updateType + "\"") && l.contains("entity=\"ItemEntity\""))
                .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no morphium_operations_duration_seconds_count for " + updateType + " in:\n" + body));
    }
}
