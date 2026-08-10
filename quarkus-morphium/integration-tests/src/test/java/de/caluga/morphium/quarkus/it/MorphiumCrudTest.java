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
 * End-to-end CRUD tests using the injected {@link Morphium} bean and the InMemDriver.
 * Covers: store, find-by-field, find-all, count, delete.
 */
@QuarkusTest
@DisplayName("Morphium CRUD operations")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumCrudTest {

    @Inject
    Morphium morphium;

    // Shared ID across ordered tests
    private static String storedId;

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("store() sets id and returns persisted entity")
    void store_setsId() {
        var item = new ItemEntity();
        item.setName("Widget");
        item.setPrice(9.99);

        morphium.store(item);

        assertThat(item.getId())
                .as("id must be assigned after store()")
                .isNotNull()
                .isNotBlank();

        storedId = item.getId();
    }

    @Test
    @Order(2)
    @DisplayName("@PreStore lifecycle hook runs on store()")
    void preStore_lifecycleHookRuns() {
        var item = new ItemEntity();
        item.setName("Gadget");
        morphium.store(item);

        // @PreStore sets tag="default" when null
        assertThat(item.getTag()).isEqualTo("default");
    }

    @Test
    @Order(3)
    @DisplayName("createQueryFor().f().eq().get() finds stored entity")
    void query_findByField() {
        ItemEntity found = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("Widget")
                .get();

        assertThat(found).isNotNull();
        assertThat(found.getPrice()).isEqualTo(9.99);
        assertThat(found.getId()).isEqualTo(storedId);
    }

    @Test
    @Order(4)
    @DisplayName("createQueryFor().asList() returns all stored entities")
    void query_findAll() {
        List<ItemEntity> all = morphium.createQueryFor(ItemEntity.class).asList();
        assertThat(all).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    @Order(5)
    @DisplayName("createQueryFor().countAll() reflects the stored count")
    void query_count() {
        long count = morphium.createQueryFor(ItemEntity.class).countAll();
        assertThat(count).isGreaterThanOrEqualTo(2);
    }

    @Test
    @Order(6)
    @DisplayName("delete() removes the entity; subsequent query returns null")
    void delete_removesEntity() {
        ItemEntity toDelete = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("Widget")
                .get();
        assertThat(toDelete).isNotNull();

        morphium.delete(toDelete);

        ItemEntity afterDelete = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("Widget")
                .get();
        assertThat(afterDelete).isNull();
    }
}
