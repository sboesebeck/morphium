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

    @Test
    @DisplayName("POST echo/{malformed-id} does not throw an unhandled exception (RESTEasy Reactive path-param conversion failure)")
    void malformedPathParamDoesNotCrashTheServer() {
        // @PathParam MorphiumId id is resolved by RESTEasy Reactive's built-in JAX-RS
        // String-constructor convention: it calls MorphiumId's public MorphiumId(String)
        // constructor directly with the raw path segment, no ParamConverter or Jackson
        // involved at all. That constructor throws IllegalArgumentException("no hex string: ...")
        // on anything that isn't a 24-character hex string.
        //
        // Verified (not assumed) what RESTEasy Reactive actually does with that exception: it
        // does NOT propagate as an unhandled 500 -- a failed String-constructor path-param
        // conversion is treated as "no matching resource method", so the response is 404. That
        // is at least not a server-error leak, but it is also not a very informative 400 Bad
        // Request for what is actually invalid input, not a missing resource. Documenting the
        // real (404) behavior here rather than an unverified assumption of 500.
        given()
            .when().post("/morphium-id/echo/{id}", "not-a-valid-hex-id")
            .then()
                .statusCode(404);
    }

    @Test
    @DisplayName("POST entity with malformed id in JSON body -> 400, not 500")
    void malformedJsonBodyIdReturnsBadRequestNotServerError() {
        // This is the actual MorphiumIdJacksonModule deserializer path (distinct from the
        // @PathParam path above, which never touches Jackson at all). Its deserialize() calls
        // new MorphiumId(hex) directly on the raw JSON string value; Jackson wraps that
        // IllegalArgumentException as a JsonMappingException during body parsing, and RESTEasy
        // Reactive's default exception mapping for a body-parsing failure IS 400 Bad Request
        // (verified against the actual response below, not assumed).
        given()
            .contentType("application/json")
            .body("{\"id\":\"not-a-valid-hex-id\",\"name\":\"whatever\"}")
            .when().post("/morphium-id/entity")
            .then()
                .statusCode(400);
    }
}
