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
 * MOR-017: {@code @Query("DELETE FROM Entity WHERE ...")} methods in the generated repository, with the return types void, int and long.
 * The subclasses run them on the in-memory driver and on a real MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumDataJdqlDeleteTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void seed() {
        create("C1", 100.0, "OPEN");
        create("C2", 250.0, "OPEN");
        create("C3", 50.0, "CLOSED");
        create("C4", 300.0, "CLOSED");
    }

    private void create(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }

    private long count() {
        return morphium.createQueryFor(OrderEntity.class).countAll();
    }

    @Test
    @DisplayName("a long result is the number of deleted documents")
    void longResult() {
        assertThat(repository.deleteJdqlByStatus("OPEN")).isEqualTo(2L);
        assertThat(count()).isEqualTo(2L);
        assertThat(repository.deleteJdqlByStatus("OPEN")).isZero();
    }

    @Test
    @DisplayName("an int result, with ordinal parameters")
    void intResult() {
        assertThat(repository.deleteJdqlOrdinal("CLOSED", 100.0)).isEqualTo(1);
        assertThat(morphium.createQueryFor(OrderEntity.class).f("customerId").eq("C3").countAll()).isZero();
        assertThat(count()).isEqualTo(3L);
    }

    @Test
    @DisplayName("a void method deletes and discards the count")
    void voidResult() {
        repository.deleteJdqlByCustomer("C2");

        assertThat(count()).isEqualTo(3L);
        assertThat(morphium.createQueryFor(OrderEntity.class).f("customerId").eq("C2").countAll()).isZero();
    }
}
