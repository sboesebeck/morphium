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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the extension correctly produces a {@link Morphium} CDI bean
 * and that it is operational (connected to the InMemDriver).
 */
@QuarkusTest
@DisplayName("Morphium CDI injection")
class MorphiumInjectionTest {

    @Inject
    Morphium morphium;

    @Test
    @DisplayName("Morphium bean is not null")
    void morphiumBeanIsProduced() {
        assertThat(morphium).isNotNull();
    }

    @Test
    @DisplayName("Morphium is connected (InMemDriver reports isConnected=true)")
    void morphiumIsConnected() {
        assertThat(morphium.getDriver().isConnected()).isTrue();
    }

    @Test
    @DisplayName("Morphium uses the configured database name")
    void morphiumUsesConfiguredDatabase() {
        assertThat(morphium.getConfig().connectionSettings().getDatabase()).isEqualTo("it-db");
    }
}
