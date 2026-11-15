package de.caluga.morphium.showcase;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The version badge reads {@code showcase.version.*}, which Maven filters into application.properties. The properties it used once
 * ({@code morphium.version}, {@code quarkus-morphium.version}) do not exist in this reactor; the badge showed the placeholders.
 */
@QuarkusTest
class VersionBadgeTest {

    @Test
    void theStartPageShowsTheVersionNotAPlaceholder() {
        String body = given().when().get("/").then().statusCode(200).extract().asString();
        assertFalse(body.matches("(?s).*@[\\w.-]+\\.version@.*"), "unresolved version placeholder on the start page");
        assertTrue(body.matches("(?s).*Morphium\\s+\\d+\\.\\d+.*"), "a version number after 'Morphium' is expected");
    }
}
