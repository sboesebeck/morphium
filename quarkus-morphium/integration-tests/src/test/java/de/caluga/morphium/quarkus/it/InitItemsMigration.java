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
import de.caluga.morphium.quarkus.migration.Execution;
import de.caluga.morphium.quarkus.migration.MorphiumChangeUnit;
import de.caluga.morphium.quarkus.migration.RollbackExecution;

/**
 * Test migration: inserts initial items into the database.
 */
@MorphiumChangeUnit(id = "001-init-items", order = "001", author = "test")
public class InitItemsMigration {

    @Execution
    public void execute(Morphium morphium) {
        ItemEntity item = new ItemEntity();
        item.setName("Migrated Widget");
        item.setPrice(19.99);
        item.setTag("migration-v1");
        morphium.store(item);
    }

    @RollbackExecution
    public void rollback(Morphium morphium) {
        morphium.dropCollection(ItemEntity.class);
    }
}
