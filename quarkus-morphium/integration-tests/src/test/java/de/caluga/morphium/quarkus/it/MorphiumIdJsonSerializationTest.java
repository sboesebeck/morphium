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

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.quarkus.testing.InMemMorphiumTestProfile;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end acceptance test for the extension's default {@code MorphiumId} JSON
 * handling over a real REST endpoint (using {@code quarkus-rest-jackson}), with
 * no user-written serializer anywhere in the application.
 *
 * <p>Reproduces the production bug from the datona-component-library showcase:
 * before the customizer, {@code GET /morphium-id/entity/{id}} returned
 * {@code "id":{"pid":..,"counter":..,...}}, which collapsed every grid row to the
 * same key on the consumer side.
 */
@QuarkusTest
@TestProfile(InMemMorphiumTestProfile.class)
@DisplayName("MorphiumId JSON wire format over REST")
class MorphiumIdJsonSerializationTest {

    @Test
    @DisplayName("GET entity -> id is a flat hex string, not the {pid,counter,...} struct")
    void entityIdSerializesAsHexString() {
        MorphiumId id = new MorphiumId();

        given()
            .when().get("/morphium-id/entity/{id}", id.toString())
            .then()
                .statusCode(200)
                .body("id", equalTo(id.toString()))
                .body("name", equalTo("widget"))
                // The internal bean shape must not leak.
                .body("id", not(equalTo("[object Object]")));
    }

    @Test
    @DisplayName("POST echo/{id} -> hex path param parses into a real MorphiumId")
    void pathParamDeserializesFromHexString() {
        MorphiumId id = new MorphiumId();

        String echoed = given()
            .when().post("/morphium-id/echo/{id}", id.toString())
            .then()
                .statusCode(200)
                .extract().asString();

        // Round-trips via equals: the server reconstructed the same MorphiumId.
        org.assertj.core.api.Assertions.assertThat(new MorphiumId(echoed)).isEqualTo(id);
    }
}
