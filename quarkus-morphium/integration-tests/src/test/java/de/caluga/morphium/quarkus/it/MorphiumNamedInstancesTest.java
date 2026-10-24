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
import de.caluga.morphium.quarkus.MorphiumInstance;
import de.caluga.morphium.quarkus.health.MorphiumNamedReadinessCheck;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-032: two Morphium instances with different databases, the default one and one that is configured as {@code quarkus.morphium.named."second".*}, are injected and used
 * side by side.
 */
@QuarkusTest
@DisplayName("Named Morphium instances")
class MorphiumNamedInstancesTest {

    @Inject
    Morphium morphium;

    @Inject
    @MorphiumInstance("second")
    Morphium second;

    @Inject
    @Any
    Instance<Morphium> all;

    @Inject
    @Readiness
    MorphiumNamedReadinessCheck readiness;

    @AfterEach
    void clean() {
        morphium.dropCollection(ItemEntity.class);
        second.dropCollection(ItemEntity.class);
    }

    private static ItemEntity item(String name) {
        ItemEntity i = new ItemEntity();
        i.setName(name);
        i.setPrice(1.0);
        return i;
    }

    @Test
    @DisplayName("the default and the named instance have their own database and their own data")
    void twoInstancesTwoDatabases() {
        assertThat(morphium).isNotSameAs(second);
        assertThat(morphium.getConfig().connectionSettings().getDatabase()).isEqualTo("it-db");
        assertThat(second.getConfig().connectionSettings().getDatabase()).isEqualTo("it-second-db");

        morphium.store(item("in default"));
        second.store(item("in second"));
        second.store(item("also in second"));

        assertThat(morphium.createQueryFor(ItemEntity.class).countAll()).isEqualTo(1);
        assertThat(second.createQueryFor(ItemEntity.class).countAll()).isEqualTo(2);
        assertThat(morphium.createQueryFor(ItemEntity.class).f("name").eq("in second").countAll()).isZero();
        assertThat(second.createQueryFor(ItemEntity.class).f("name").eq("in second").countAll()).isEqualTo(1);
    }

    @Test
    @DisplayName("the qualifier selects the named bean also programmatically, and an unqualified injection stays the default instance")
    void programmaticLookup() {
        assertThat(all.select(new MorphiumInstance.Literal("second")).get().getConfig().connectionSettings().getDatabase()).isEqualTo("it-second-db");
        assertThat(all.select(new MorphiumInstance.Literal("second")).get()).isSameAs(second);
        assertThat(all.select(new MorphiumInstance.Literal("nobody")).isUnsatisfied()).isTrue();
    }

    @Test
    @DisplayName("the readiness check of the named instances names every instance")
    void readinessNamesTheInstances() {
        HealthCheckResponse response = readiness.call();

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
        assertThat(response.getData()).isPresent();
        assertThat(response.getData().get()).containsKey("second");
        assertThat(String.valueOf(response.getData().get().get("second"))).contains("it-second-db");
    }
}
