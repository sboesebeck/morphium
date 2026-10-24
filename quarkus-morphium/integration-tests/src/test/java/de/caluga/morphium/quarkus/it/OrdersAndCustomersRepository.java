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

import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;

import java.util.List;
import java.util.Optional;

/** MOR-077: a repository without a primary entity; every method works on the entity that it names, takes or returns */
@Repository
public interface OrdersAndCustomersRepository {

    @Insert
    OrderEntity add(OrderEntity order);

    @Insert
    CustomerEntity[] add(CustomerEntity... customers);

    @Query("DELETE FROM CustomerEntity")
    long removeCustomers();

    @Query("DELETE FROM OrderEntity WHERE status = :status")
    boolean removeOrders(@Param("status") String status);

    @Query("UPDATE OrderEntity SET amount = amount / ?2 WHERE customerId = ?1")
    long halve(String customerId, double by);

    @Query("WHERE name = ?1")
    Optional<CustomerEntity> customerByName(String name);

    @Query("WHERE status = :status ORDER BY amount")
    List<OrderEntity> ordersByStatus(@Param("status") String status);
}
