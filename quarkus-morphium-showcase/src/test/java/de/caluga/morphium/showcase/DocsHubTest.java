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

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * The docs hub shows the pages of {@code docs/} of this repository, copied at build time (there is no second copy in the module), without the research notes and the task ledger,
 * with relative links resolved to the hub.
 */
@QuarkusTest
class DocsHubTest {

    @Test
    void theHubListsThePagesOfTheRepositoryDocs() {
        given().when().get("/docs").then().statusCode(200)
                .body(containsString("Migrating from Morphium 6 to 7"))
                .body(containsString("Updates: Pitfalls of Raw Maps"))
                .body(not(containsString("Task ledger")));
    }

    @Test
    void theMigrationPageIsRenderedAndItsLinksPointIntoTheHub() {
        given().when().get("/docs/howtos/migration-v6-to-v7").then().statusCode(200)
                .body(containsString("A rejected write raises"))
                .body(containsString("href=\"/docs/howtos/update-pitfalls\""))
                .body(containsString("href=\"/docs/howtos/migration-v6_2-to-v6_3\""));
    }

    @Test
    void aLinkToAParentDirectoryIsResolved() {
        given().when().get("/docs/howtos/update-pitfalls").then().statusCode(200)
                .body(not(containsString("href=\"../")))
                .body(not(containsString(".md\"")));
    }

    @Test
    void researchNotesAndTheLedgerAreNotInTheHub() {
        given().when().get("/docs/research/jakarta-data-1.1").then().statusCode(200).body(containsString("Document not found"));
        given().when().get("/docs/tasks/ledger").then().statusCode(200).body(containsString("Document not found"));
    }
}
