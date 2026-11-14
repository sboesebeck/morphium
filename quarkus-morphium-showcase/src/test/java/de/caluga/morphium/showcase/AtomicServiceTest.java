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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.showcase.atomic.AtomicService;
import de.caluga.morphium.showcase.atomic.entity.JobState;
import de.caluga.morphium.showcase.atomic.entity.JobTicket;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;

/** The "Atomic Updates" page (7.0): the operations and what they answer. */
@QuarkusTest
class AtomicServiceTest {

    @Inject
    AtomicService service;

    @Inject
    Morphium morphium;

    @BeforeEach
    void reset() {
        service.reset();
    }

    private JobTicket ticket(String id) {
        return morphium.createQueryFor(JobTicket.class).f(JobTicket.Fields.id).eq(id).get();
    }

    @Test
    void ofTwoWorkersExactlyOneClaimsATicket() {
        assertThat(service.claim("T-1", "alice")).contains("alice claimed T-1").contains("attempt 1");
        assertThat(service.claim("T-1", "bob")).contains("bob lost");

        JobTicket t = ticket("T-1");
        assertThat(t.getState()).isEqualTo(JobState.RUNNING);
        assertThat(t.getOwner()).isEqualTo("alice");
        assertThat(t.getAttempts()).isEqualTo(1);
        assertThat(t.getStartedAt()).as("an Instant, mapped by the builder").isNotNull();
        assertThat(t.getChangedAt()).as("a @LastChange on the field only is set by the update").isPositive();
    }

    @Test
    void aTicketThatIsNotNewCannotBeClaimed() {
        assertThat(service.claim("T-3", "alice")).contains("alice lost");
        assertThat(ticket("T-3").getOwner()).isEqualTo("carol");
    }

    @Test
    void theLeaseGoesToTheFirstAndIsFreeAfterTheRelease() {
        assertThat(service.acquireLease("alice")).contains("alice holds the lease");
        assertThat(service.acquireLease("bob")).contains("did not get it").contains("alice");
        assertThat(service.releaseLease("bob")).contains("does not hold");
        assertThat(service.releaseLease("alice")).contains("released");
        assertThat(service.acquireLease("bob")).contains("bob holds the lease");
        assertThat(service.currentLease().getOwnerName()).isEqualTo("bob");
    }

    @Test
    void aDerivedUpdateSetsTheOwnerOfEveryTicketInTheState() {
        assertThat(service.assignAll(JobState.NEW, "dave")).isEqualTo(2);
        assertThat(ticket("T-1").getOwner()).isEqualTo("dave");
        assertThat(ticket("T-2").getOwner()).isEqualTo("dave");
        assertThat(ticket("T-3").getOwner()).as("another state is untouched").isEqualTo("carol");
    }

    @Test
    void aDerivedUpdateOfOneTicketAnswersWithTheTicketOrSaysItDidNotMatch() {
        assertThat(service.assignOne("T-2", JobState.NEW, "erin")).contains("T-2 belongs to erin");
        assertThat(service.assignOne("T-2", JobState.DONE, "frank")).contains("nothing was changed");
        assertThat(ticket("T-2").getOwner()).isEqualTo("erin");
    }

    @Test
    void aDuplicateCodeIsARejectedWriteThatRaisesAndChangesNothing() {
        assertThat(service.changeCode("T-1", "A-200")).contains("Rejected").contains("duplicate key");
        assertThat(ticket("T-1").getCode()).isEqualTo("A-100");

        assertThat(service.changeCode("T-1", "A-150")).contains("A-150 now");
        assertThat(ticket("T-1").getCode()).isEqualTo("A-150");
    }

    @Test
    void thePageIsRendered() {
        given().when().get("/atomic").then().statusCode(200).body(containsString("Atomic Updates"));
    }

    @Test
    void theLearnTabIsRendered() {
        given().when().get("/atomic/tab/learn").then().statusCode(200).body(containsString("Query.modify()"));
    }

    @Test
    void theDemoTabShowsTheTicketsAndTheLeaseButtons() {
        given().when().get("/atomic/tab/demo").then().statusCode(200).body(containsString("T-1")).body(containsString("alice acquires"));
    }

    @Test
    void theButtonsAnswerWithTheOutcome() {
        given().formParam("worker", "bob").when().post("/atomic/tickets/T-2/claim").then().statusCode(200).body(containsString("bob claimed T-2"));
        given().formParam("state", "NEW").formParam("owner", "gina").when().post("/atomic/assign").then().statusCode(200).body(containsString("belong to gina"));
    }
}
