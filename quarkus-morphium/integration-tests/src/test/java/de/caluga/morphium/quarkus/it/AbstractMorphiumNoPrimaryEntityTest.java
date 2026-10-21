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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-077: repositories that extend nothing, in the generated implementation. The lifecycle methods run under any name and in every shape; with several entities every method works on the one
 * it names, takes or returns. The subclasses run the tests on the in-memory driver and on a real MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumNoPrimaryEntityTest {

    @Inject
    NoPrimaryOrderRepository orders;

    @Inject
    OrdersAndCustomersRepository both;

    @Inject
    Morphium morphium;

    private static OrderEntity order(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        return order;
    }

    private static CustomerEntity customer(String name) {
        var customer = new CustomerEntity();
        customer.setName(name);
        return customer;
    }

    @Test
    @DisplayName("insert, save, update and delete under any name and in every shape")
    void lifecycleInEveryShape() {
        OrderEntity one = orders.add(order("C1", 10, "OPEN"));
        assertThat(one.getId()).isNotNull();
        assertThat(orders.addAll(List.of(order("C2", 20, "OPEN"), order("C3", 30, "CLOSED")))).hasSize(2);
        OrderEntity[] array = orders.addArray(order("C4", 40, "CLOSED"), order("C5", 50, "CLOSED"));
        assertThat(array).hasSize(2);
        orders.saveOne(order("C6", 60, "OPEN"));
        assertThat(morphium.createQueryFor(OrderEntity.class).countAll()).isEqualTo(6);

        one.setAmount(11);
        assertThat(orders.changeAll(List.of(one))).isTrue();
        assertThat(morphium.findById(OrderEntity.class, one.getId(), null).getAmount()).isEqualTo(11);

        assertThat(orders.removeOne(array[0])).isEqualTo(1L);
        orders.remove(List.of(array[1], one));
        assertThat(morphium.createQueryFor(OrderEntity.class).countAll()).isEqualTo(3);
    }

    @Test
    @DisplayName("derived and query methods use the entity of the lifecycle methods")
    void primaryEntityOfTheLifecycleMethods() {
        orders.addAll(List.of(order("C1", 10, "OPEN"), order("C2", 20, "OPEN"), order("C3", 30, "CLOSED")));

        assertThat(orders.countByStatus("OPEN")).isEqualTo(2L);
        assertThat(orders.findByStatus("CLOSED")).extracting(OrderEntity::getCustomerId).containsExactly("C3");
        assertThat(orders.byCustomer("C2")).isPresent();
        assertThat(orders.dropStatus("OPEN")).isEqualTo(2L);
        assertThat(morphium.createQueryFor(OrderEntity.class).countAll()).isEqualTo(1);
    }

    @Test
    @DisplayName("every method of a repository with several entities works on its own")
    void severalEntities() {
        both.add(order("C1", 100, "OPEN"));
        both.add(order("C2", 50, "OPEN"));
        both.add(order("C3", 70, "GONE"));
        CustomerEntity[] customers = both.add(customer("Ada"), customer("Grace"));
        assertThat(customers).hasSize(2);

        assertThat(both.ordersByStatus("OPEN")).extracting(OrderEntity::getCustomerId).containsExactly("C2", "C1");
        assertThat(both.customerByName("Grace")).isPresent();
        assertThat(both.customerByName("Nobody")).isEmpty();

        assertThat(both.halve("C1", 4)).isEqualTo(1L);
        assertThat(both.ordersByStatus("OPEN")).extracting(OrderEntity::getAmount).containsExactly(25.0, 50.0);

        assertThat(both.removeOrders("GONE")).isTrue();
        assertThat(both.removeOrders("GONE")).isFalse();
        assertThat(both.removeCustomers()).isEqualTo(2L);
        assertThat(morphium.createQueryFor(CustomerEntity.class).countAll()).isZero();
        assertThat(morphium.createQueryFor(OrderEntity.class).countAll()).isEqualTo(2);
    }
}
