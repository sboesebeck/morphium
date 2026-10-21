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

import jakarta.data.repository.Delete;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;

import java.util.List;
import java.util.Optional;

/** MOR-077: a repository that extends nothing; its primary entity is the one of its lifecycle methods */
@Repository
public interface NoPrimaryOrderRepository {

    @Insert
    OrderEntity add(OrderEntity order);

    @Insert
    List<OrderEntity> addAll(List<OrderEntity> orders);

    @Insert
    OrderEntity[] addArray(OrderEntity... orders);

    @Save
    void saveOne(OrderEntity order);

    @Update
    boolean changeAll(List<OrderEntity> orders);

    @Delete
    void remove(List<OrderEntity> orders);

    @Delete
    long removeOne(OrderEntity order);

    long countByStatus(String status);

    List<OrderEntity> findByStatus(String status);

    @Query("DELETE FROM OrderEntity WHERE status = :status")
    long dropStatus(@Param("status") String status);

    @Query("WHERE customerId = ?1")
    Optional<OrderEntity> byCustomer(String customerId);
}
