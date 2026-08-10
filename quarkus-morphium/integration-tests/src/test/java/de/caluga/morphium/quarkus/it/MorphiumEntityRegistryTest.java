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

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Quarkus build-time entity discovery (Jandex scan in
 * {@code MorphiumProcessor}) correctly pre-registers {@code @Entity} and
 * {@code @Embedded} classes via {@code AnnotationAndReflectionHelper.registerTypeIds()}.
 *
 * <p>This is an explicit test for the pre-registration flow:
 * the Processor discovers entities at build time, the Recorder stores
 * class names, and the Producer builds a typeId map and registers it.
 */
@QuarkusTest
@DisplayName("Build-time entity pre-registration (registerTypeIds)")
class MorphiumEntityRegistryTest {

    @Inject
    Morphium morphium;

    @Test
    @DisplayName("TypeId resolution works for pre-registered @Entity")
    void typeIdResolution_worksForEntity() throws Exception {
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        Class<?> resolved = arh.getClassForTypeId(CustomerEntity.class.getName());
        assertThat(resolved).isEqualTo(CustomerEntity.class);
    }

    @Test
    @DisplayName("TypeId resolution works for pre-registered @Embedded")
    void typeIdResolution_worksForEmbedded() throws Exception {
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        Class<?> resolved = arh.getClassForTypeId(AddressEmbedded.class.getName());
        assertThat(resolved).isEqualTo(AddressEmbedded.class);
    }

    @Test
    @DisplayName("ObjectMapper resolves collection name for pre-registered @Entity")
    void objectMapper_resolvesCollectionName() {
        String collName = morphium.getMapper().getCollectionName(CustomerEntity.class);
        assertThat(collName).isEqualTo("it_customers");
    }

    @Test
    @DisplayName("ObjectMapper resolves class for pre-registered collection name")
    void objectMapper_resolvesClassForCollectionName() {
        Class<?> resolved = morphium.getMapper().getClassForCollectionName("it_customers");
        assertThat(resolved).isEqualTo(CustomerEntity.class);
    }

    @Test
    @DisplayName("TypeId for OrderEntity resolves correctly")
    void typeIdResolution_worksForOrderEntity() throws Exception {
        AnnotationAndReflectionHelper arh = new AnnotationAndReflectionHelper(true);
        Class<?> resolved = arh.getClassForTypeId(OrderEntity.class.getName());
        assertThat(resolved).isEqualTo(OrderEntity.class);
    }
}
