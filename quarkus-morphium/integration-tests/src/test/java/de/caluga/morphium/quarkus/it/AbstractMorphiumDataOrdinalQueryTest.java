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
import jakarta.data.Sort;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-024: {@code @Query} methods with ordinal parameters ({@code ?1}) in the generated repository. The subclasses run them on the
 * in-memory driver and on a real MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumDataOrdinalQueryTest {

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

    @Test
    @DisplayName("?1 and ?2 are the first and the second method parameter")
    void ordinalsAreBound() {
        List<OrderEntity> result = repository.queryOrdinal("OPEN", 150.0);

        assertThat(result).extracting(OrderEntity::getCustomerId).containsExactly("C2");
    }

    @Test
    @DisplayName("BETWEEN with ordinals")
    void between() {
        assertThat(repository.queryOrdinalRange(100.0, 250.0)).extracting(OrderEntity::getCustomerId).containsExactly("C2", "C1");
    }

    @Test
    @DisplayName("a Sort parameter is not counted: ?1 is the status")
    void sortIsNotCounted() {
        assertThat(repository.queryOrdinalSorted("CLOSED", Sort.desc("amount"))).extracting(OrderEntity::getCustomerId).containsExactly("C4", "C3");
    }

    @Test
    @DisplayName("the ordinals need not follow the order of the parameters")
    void reversed() {
        assertThat(repository.countOrdinalReversed(100.0, "OPEN")).isEqualTo(2L);
    }

    @Test
    @DisplayName("countAll() has no By and counts every document (MOR-076)")
    void countAll() {
        assertThat(repository.countAll()).isEqualTo(4L);
    }

    @Test
    @DisplayName("findFirstBy returns one, findFirst2By at most two, in the order of the method name (MOR-079)")
    void findFirst() {
        assertThat(repository.findFirstByStatusOrderByAmountDesc("CLOSED")).get().extracting(OrderEntity::getCustomerId).isEqualTo("C4");
        assertThat(repository.findFirstByStatusOrderByAmountDesc("NOBODY")).isEmpty();
        assertThat(repository.findFirst2ByStatusOrderByAmountAsc("OPEN")).extracting(OrderEntity::getCustomerId).containsExactly("C1", "C2");
        assertThat(repository.findFirst2ByStatusOrderByAmountAsc("CLOSED")).extracting(OrderEntity::getCustomerId).containsExactly("C3", "C4");
        morphium.store(newOrder("C5", 10.0, "CLOSED"));
        assertThat(repository.findFirst2ByStatusOrderByAmountAsc("CLOSED")).extracting(OrderEntity::getCustomerId).containsExactly("C5", "C3");
    }

    private OrderEntity newOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        return order;
    }

    @Test
    @DisplayName("a method may return an array of the entity (MOR-080)")
    void arrays() {
        assertThat(repository.findByStatusOrderByAmountAsc("OPEN")).extracting(OrderEntity::getCustomerId).containsExactly("C1", "C2");
        assertThat(repository.arrayByStatus("CLOSED")).hasSize(2);
        assertThat(repository.arrayQueryByStatus("CLOSED")).extracting(OrderEntity::getCustomerId).containsExactly("C4", "C3");
        assertThat(repository.arrayByStatus("NOBODY")).isEmpty();
    }

    @Test
    @DisplayName("SELECT field answers the values of the field when the method returns them (MOR-083)")
    void selectedValues() {
        assertThat(repository.customersByStatus("OPEN")).containsExactly("C1", "C2");
        assertThat(repository.amountOf("C1")).contains(repository.findByStatusOrderByAmountAsc("OPEN")[0].getAmount());
        assertThat(repository.amountOf("NOBODY")).isEmpty();
        assertThat(repository.customerOf("C2")).isEqualTo("C2");
    }

    @Test
    @DisplayName("several Sort parameters, Sort... and a static order that comes first (MOR-081)")
    void sorts() {
        create("C5", 100.0, "OPEN");   // C1 and C5 have the same amount

        assertThat(repository.findByStatus("OPEN", Sort.asc("amount"), Sort.desc("customerId"))).extracting(OrderEntity::getCustomerId).containsExactly("C5", "C1", "C2");
        assertThat(repository.findByStatus("OPEN", Sort.asc("amount"), Sort.desc("customerId"), Sort.asc("id"))).extracting(OrderEntity::getCustomerId).containsExactly("C5", "C1", "C2");
        assertThat(repository.findByStatusOrderByCustomerIdAsc("OPEN", jakarta.data.Order.by(Sort.desc("amount"), Sort.desc("customerId"))))
            .extracting(OrderEntity::getCustomerId).containsExactly("C1", "C2", "C5");
    }
}
