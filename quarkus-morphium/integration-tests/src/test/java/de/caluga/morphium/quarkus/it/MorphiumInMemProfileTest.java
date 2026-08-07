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
import de.caluga.morphium.quarkus.testing.InMemMorphiumTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link InMemMorphiumTestProfile} from the {@code quarkus-morphium-testing}
 * module correctly overrides configuration and that all Morphium operations work under
 * the profile.
 *
 * <p>This test uses a different Quarkus application context from the default integration
 * tests ({@code morphium.database=inmem-test} instead of {@code it-db}). Quarkus restarts
 * the context once when switching profiles.
 */
@QuarkusTest
@TestProfile(InMemMorphiumTestProfile.class)
@DisplayName("InMemMorphiumTestProfile (quarkus-morphium-testing)")
class MorphiumInMemProfileTest {

    @Inject
    Morphium morphium;

    @Test
    @DisplayName("profile overrides quarkus.morphium.database to 'inmem-test'")
    void profile_overridesDatabase() {
        String db = ConfigProvider.getConfig()
                .getValue("quarkus.morphium.database", String.class);
        assertThat(db).isEqualTo("inmem-test");
    }

    @Test
    @DisplayName("profile keeps quarkus.morphium.driver-name as InMemDriver")
    void profile_driverIsInMem() {
        String driver = ConfigProvider.getConfig()
                .getValue("quarkus.morphium.driver-name", String.class);
        assertThat(driver).isEqualTo("InMemDriver");
    }

    @Test
    @DisplayName("profile disables Dev Services")
    void profile_devServicesDisabled() {
        String enabled = ConfigProvider.getConfig()
                .getValue("quarkus.morphium.devservices.enabled", String.class);
        assertThat(enabled).isEqualTo("false");
    }

    @Test
    @DisplayName("Morphium bean is injectable and connected under the profile")
    void morphium_isConnected() {
        assertThat(morphium).isNotNull();
        assertThat(morphium.getDriver().isConnected()).isTrue();
    }

    @Test
    @DisplayName("full CRUD cycle works under InMemMorphiumTestProfile")
    void crudCycle_worksUnderProfile() {
        morphium.dropCollection(ItemEntity.class);

        var item = new ItemEntity();
        item.setName("inm-profile-item");
        item.setPrice(3.14);
        morphium.store(item);

        assertThat(item.getId()).isNotNull();
        assertThat(item.getVersion()).isEqualTo(1L);

        var found = morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("inm-profile-item").get();
        assertThat(found).isNotNull();
        assertThat(found.getPrice()).isEqualTo(3.14);

        morphium.delete(found);
        assertThat(morphium.createQueryFor(ItemEntity.class)
                .f("name").eq("inm-profile-item").get()).isNull();
    }
}
