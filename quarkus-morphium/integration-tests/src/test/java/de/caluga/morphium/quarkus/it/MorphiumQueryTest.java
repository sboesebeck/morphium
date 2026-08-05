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
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Morphium query operations: filtering, sorting, pagination,
 * count, and the {@code in()} operator. All tests run against the InMemDriver.
 */
@QuarkusTest
@DisplayName("Morphium query operations")
class MorphiumQueryTest {

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(OrderEntity.class);
        morphium.ensureIndicesFor(OrderEntity.class);
        store("C1", 100.0, "OPEN");
        store("C2", 200.0, "OPEN");
        store("C3",  50.0, "CLOSED");
        store("C1", 300.0, "CLOSED");
    }

    @Test
    @DisplayName("f().eq() filters by a single field value")
    void filterByCustomer() {
        var results = morphium.createQueryFor(OrderEntity.class)
                .f("customer_id").eq("C1").asList();

        assertThat(results).hasSize(2)
                .allSatisfy(o -> assertThat(o.getCustomerId()).isEqualTo("C1"));
    }

    @Test
    @DisplayName("f().eq() on status filters correctly")
    void filterByStatus() {
        var open = morphium.createQueryFor(OrderEntity.class)
                .f("status").eq("OPEN").asList();

        assertThat(open).hasSize(2)
                .allSatisfy(o -> assertThat(o.getStatus()).isEqualTo("OPEN"));
    }

    @Test
    @DisplayName("f().gt() returns only items with amount > threshold")
    void filterByAmountGreaterThan() {
        var results = morphium.createQueryFor(OrderEntity.class)
                .f("amount").gt(150.0).asList();

        assertThat(results).hasSize(2)
                .allSatisfy(o -> assertThat(o.getAmount()).isGreaterThan(150.0));
    }

    @Test
    @DisplayName("f().lt() returns only items with amount < threshold")
    void filterByAmountLessThan() {
        var results = morphium.createQueryFor(OrderEntity.class)
                .f("amount").lt(100.0).asList();

        assertThat(results).hasSize(1)
                .first().satisfies(o -> assertThat(o.getAmount()).isEqualTo(50.0));
    }

    @Test
    @DisplayName("sort() ascending orders results by amount")
    void sortAscendingByAmount() {
        var sorted = morphium.createQueryFor(OrderEntity.class)
                .sort("amount").asList();

        assertThat(sorted).extracting(OrderEntity::getAmount)
                .containsExactly(50.0, 100.0, 200.0, 300.0);
    }

    @Test
    @DisplayName("sort() descending orders results by amount")
    void sortDescendingByAmount() {
        var sorted = morphium.createQueryFor(OrderEntity.class)
                .sort("-amount").asList();

        assertThat(sorted).extracting(OrderEntity::getAmount)
                .containsExactly(300.0, 200.0, 100.0, 50.0);
    }

    @Test
    @DisplayName("limit() restricts the result count")
    void limitResults() {
        var limited = morphium.createQueryFor(OrderEntity.class)
                .sort("amount").limit(2).asList();

        assertThat(limited).hasSize(2);
        assertThat(limited.get(0).getAmount()).isEqualTo(50.0);
        assertThat(limited.get(1).getAmount()).isEqualTo(100.0);
    }

    @Test
    @DisplayName("skip() skips the first N results")
    void skipResults() {
        var paged = morphium.createQueryFor(OrderEntity.class)
                .sort("amount").skip(2).asList();

        assertThat(paged).hasSize(2);
        assertThat(paged.get(0).getAmount()).isEqualTo(200.0);
        assertThat(paged.get(1).getAmount()).isEqualTo(300.0);
    }

    @Test
    @DisplayName("countAll() on a filtered query returns the matching count")
    void countFiltered() {
        long count = morphium.createQueryFor(OrderEntity.class)
                .f("status").eq("CLOSED").countAll();

        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("f().in() matches any of the given values")
    void inOperator() {
        var results = morphium.createQueryFor(OrderEntity.class)
                .f("customer_id").in(List.of("C1", "C3")).asList();

        // C1 has 2 orders, C3 has 1
        assertThat(results).hasSize(3)
                .allSatisfy(o -> assertThat(o.getCustomerId()).isIn("C1", "C3"));
    }

    @Test
    @DisplayName("query on empty collection returns empty list")
    void emptyCollectionReturnsEmptyList() {
        morphium.dropCollection(OrderEntity.class);

        var results = morphium.createQueryFor(OrderEntity.class).asList();

        assertThat(results).isEmpty();
    }

    // ── helper ───────────────────────────────────────────────────────────────

    private void store(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
