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
package de.caluga.morphium.showcase;

import de.caluga.morphium.driver.MorphiumId;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end verification of the {@code quarkus-morphium} extension's automatic
 * {@code MorphiumId} JSON (de)serialization, exercised through
 * {@link de.caluga.morphium.showcase.api.MorphiumIdJsonResource}.
 *
 * <p>Asserts that {@code Product.id} (a {@code @Id MorphiumId} field) appears on the wire as a
 * plain 24-character hex string -- never as the nested {@code {pid, counter, machineId, bytes,
 * time}} struct that a generic bean-introspecting JSON mapper would otherwise produce -- and that
 * a hex string in a request body parses back into a working {@code MorphiumId}. This application
 * registers no custom {@code MorphiumId} serializer anywhere; the behavior verified here comes
 * entirely from the extension's {@code MorphiumIdJacksonModule}.</p>
 */
@QuarkusTest
class MorphiumIdJsonResourceTest {

    private static final String HEX_24 = "^[0-9a-fA-F]{24}$";

    @Test
    void listProductsSerializesIdAsPlainHexString() {
        given()
            .when().get("/api/morphium-id-json/products")
            .then()
                .statusCode(200)
                .contentType("application/json")
                .body("$.size()", org.hamcrest.Matchers.greaterThan(0))
                // The id of every element must be a plain string matching the hex pattern,
                // not a JSON object with pid/counter/machineId/bytes/time keys.
                .body("id", org.hamcrest.Matchers.everyItem(matchesPattern(HEX_24)));
    }

    @Test
    void getProductByIdReturnsFlatHexStringId() {
        String firstId = given()
            .when().get("/api/morphium-id-json/products")
            .then()
                .statusCode(200)
            .extract().path("id[0]");

        given()
            .when().get("/api/morphium-id-json/products/{id}", firstId)
            .then()
                .statusCode(200)
                .body("id", equalTo(firstId))
                .body("id", matchesPattern(HEX_24))
                // Guard against the internal bean shape ever leaking back in.
                .body("id", not(equalTo("[object Object]")));
    }

    @Test
    void unknownIdReturns404() {
        MorphiumId randomId = new MorphiumId();

        given()
            .when().get("/api/morphium-id-json/products/{id}", randomId.toString())
            .then()
                .statusCode(404);
    }

    @Test
    void echoRoundTripsHexStringIdThroughRealMorphiumId() {
        MorphiumId id = new MorphiumId();
        String body = "{\"id\":\"" + id + "\",\"name\":\"Echo Test\",\"description\":\"d\","
                + "\"price\":1.5,\"stock\":3,\"tags\":[]}";

        given()
            .contentType("application/json")
            .body(body)
            .when().post("/api/morphium-id-json/products/echo")
            .then()
                .statusCode(200)
                .contentType("application/json")
                // Round-tripped through MorphiumId deserialization and back to the same hex string.
                .body("id", equalTo(id.toString()))
                .body("name", equalTo("Echo Test"));
    }

    @Test
    void malformedHexIdInRequestBodyReturns400NotServerError() {
        given()
            .contentType("application/json")
            .body("{\"id\":\"not-a-valid-hex-id\",\"name\":\"whatever\"}")
            .when().post("/api/morphium-id-json/products/echo")
            .then()
                .statusCode(400);
    }
}
