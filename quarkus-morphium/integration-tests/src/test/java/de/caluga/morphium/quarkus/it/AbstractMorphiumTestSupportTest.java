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
import de.caluga.morphium.quarkus.testing.MorphiumTestSupport;
import de.caluga.morphium.quarkus.testing.ResetMorphiumExtension;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-034: an application-style test with the helpers of the testing module. {@link ResetMorphiumExtension}
 * empties the database before each test, so every test starts clean whatever ran before it; the tests
 * write to the same collection on purpose. The subclasses run it on the in-memory driver and on a real MongoDB.
 */
@ExtendWith(ResetMorphiumExtension.class)
abstract class AbstractMorphiumTestSupportTest {

    @Inject
    Morphium morphium;

    private static ItemEntity item(String name, double price) {
        ItemEntity item = new ItemEntity();
        item.setName(name);
        item.setPrice(price);
        return item;
    }

    @Test
    @DisplayName("first test: starts with an empty database and leaves data behind")
    void firstTestStartsEmpty() {
        assertThat(morphium.createQueryFor(ItemEntity.class).countAll()).isZero();
        morphium.store(item("left-behind-1", 1.0));
        morphium.store(item("left-behind-2", 2.0));
    }

    @Test
    @DisplayName("second test: starts with an empty database and leaves data behind")
    void secondTestStartsEmpty() {
        assertThat(morphium.createQueryFor(ItemEntity.class).countAll()).isZero();
        morphium.store(item("left-behind-3", 3.0));
    }

    @Test
    @DisplayName("fixtures are stored like the application stores them")
    void fixturesAreLoaded() {
        List<ItemEntity> loaded = MorphiumTestSupport.load(morphium, item("a", 1.0), item("b", 2.0));

        assertThat(loaded).extracting(ItemEntity::getName).containsExactly("a", "b");
        assertThat(loaded).allSatisfy(i -> assertThat(i.getId()).isNotNull());
        assertThat(morphium.createQueryFor(ItemEntity.class).f("name").eq("b").get().getPrice()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("reset empties the collections but keeps them, and with them their indexes")
    void resetKeepsTheCollections() throws Exception {
        MorphiumTestSupport.load(morphium, item("x", 1.0));
        String collection = morphium.getMapper().getCollectionName(ItemEntity.class);

        int emptied = MorphiumTestSupport.reset(morphium);

        assertThat(emptied).isPositive();
        assertThat(morphium.createQueryFor(ItemEntity.class).countAll()).isZero();
        assertThat(morphium.getDriver().listCollections(morphium.getConfig().connectionSettings().getDatabase(), null)).contains(collection);
    }
}
