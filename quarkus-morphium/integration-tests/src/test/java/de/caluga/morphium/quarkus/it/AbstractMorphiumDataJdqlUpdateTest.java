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
import de.caluga.morphium.quarkus.testing.ResetMorphiumExtension;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-018: {@code @Query("UPDATE Entity SET ... WHERE ...")} methods in the generated repository, with the return types long, int and void. The subclasses
 * run them on the in-memory driver and on a real MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumDataJdqlUpdateTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void seed() {
        create("C1", 100.0, "OPEN");
        create("C2", 250.0, "OPEN");
        create("C3", 50.0, "CLOSED");
    }

    private void create(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }

    private OrderEntity byCustomer(String customerId) {
        return morphium.createQueryFor(OrderEntity.class).f("customerId").eq(customerId).get();
    }

    @Test
    @DisplayName("a value and an increment in one statement, the result is the matched count")
    void setAndIncrement() {
        assertThat(repository.updateJdqlStatusAndAmount("PAID", "C1")).isEqualTo(1L);

        OrderEntity c1 = byCustomer("C1");
        assertThat(c1.getStatus()).isEqualTo("PAID");
        assertThat(c1.getAmount()).isEqualTo(101.0);
        assertThat(byCustomer("C2").getStatus()).isEqualTo("OPEN");
        assertThat(repository.updateJdqlStatusAndAmount("PAID", "NOBODY")).isZero();
    }

    @Test
    @DisplayName("multiply, ordinal parameters, int result")
    void multiply() {
        assertThat(repository.updateJdqlScaleByStatus("OPEN", 2.0)).isEqualTo(2);

        assertThat(byCustomer("C1").getAmount()).isEqualTo(200.0);
        assertThat(byCustomer("C2").getAmount()).isEqualTo(500.0);
        assertThat(byCustomer("C3").getAmount()).isEqualTo(50.0);
    }

    @Test
    @DisplayName("a literal and NULL, void result")
    void literalAndNull() {
        repository.updateJdqlArchive("OPEN");

        assertThat(morphium.createQueryFor(OrderEntity.class).f("status").eq("ARCHIVED").countAll()).isEqualTo(2L);
        assertThat(morphium.createQueryFor(OrderEntity.class).f("customerId").eq(null).countAll()).isEqualTo(2L);
        assertThat(byCustomer("C3").getStatus()).isEqualTo("CLOSED");
    }
}
