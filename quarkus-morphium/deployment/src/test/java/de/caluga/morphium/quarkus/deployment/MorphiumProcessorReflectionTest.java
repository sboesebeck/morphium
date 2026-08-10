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
package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.DefaultNameProvider;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link MorphiumProcessor#registerSubclasses} and
 * {@link MorphiumProcessor#registerCustomNameProvider} (should-fix #10).
 *
 * <p>Builds a real Jandex index from actual compiled test-fixture classes (not a mock), so
 * these tests exercise the exact same {@code IndexView}/{@code AnnotationInstance} API contract
 * the real build step relies on.
 */
@DisplayName("MorphiumProcessor — native-image reflection registration (should-fix #10)")
class MorphiumProcessorReflectionTest {

    /** Base entity, annotated. */
    @Entity
    public static class BaseAnimal {
        @Id
        public String id;
    }

    /** Subclass with NO annotation of its own -- Morphium treats it as an entity anyway
     *  via AnnotationAndReflectionHelper.isAnnotationPresentInHierarchy(). */
    public static class DogSubclass extends BaseAnimal {
        public String breed;
    }

    /** Transitive subclass, two levels down. */
    public static class PuppySubclass extends DogSubclass {
        public int ageMonths;
    }

    /** Custom NameProvider a user might point @Entity(nameProvider = ...) at. */
    public static class CustomNameProvider implements NameProvider {
        @Override
        public String getCollectionName(Class<?> type, de.caluga.morphium.objectmapping.MorphiumObjectMapper om,
                                         boolean translateCamelCase, boolean useFQN,
                                         String specifiedName, de.caluga.morphium.Morphium morphium) {
            return "custom";
        }
    }

    @Entity(nameProvider = CustomNameProvider.class)
    public static class EntityWithCustomNameProvider {
        @Id
        public String id;
    }

    @Entity // uses the default nameProvider() = DefaultNameProvider.class
    public static class EntityWithDefaultNameProvider {
        @Id
        public String id;
    }

    private static IndexView buildIndex(Class<?>... classes) throws IOException {
        Indexer indexer = new Indexer();
        for (Class<?> c : classes) {
            String resource = c.getName().replace('.', '/') + ".class";
            try (InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
                indexer.index(in);
            }
        }
        return indexer.complete();
    }

    private static class CollectingProducer implements BuildProducer<ReflectiveClassBuildItem> {
        final Set<String> registeredClassNames = new HashSet<>();

        @Override
        public void produce(ReflectiveClassBuildItem item) {
            registeredClassNames.addAll(item.getClassNames());
        }
    }

    @Test
    @DisplayName("registerSubclasses: registers direct and transitive subclasses, not just the annotated base")
    void registerSubclasses_registersDirectAndTransitiveSubclasses() throws IOException {
        IndexView index = buildIndex(BaseAnimal.class, DogSubclass.class, PuppySubclass.class);
        CollectingProducer producer = new CollectingProducer();
        MorphiumProcessor processor = new MorphiumProcessor();

        ClassInfo baseInfo = index.getClassByName(DotName.createSimple(BaseAnimal.class.getName()));
        processor.registerSubclasses(baseInfo, index, producer, new HashSet<>());

        assertThat(producer.registeredClassNames)
                .as("both the direct subclass and the transitive (grand-child) subclass must be registered")
                .contains(DogSubclass.class.getName(), PuppySubclass.class.getName());
    }

    @Test
    @DisplayName("registerCustomNameProvider: registers a custom nameProvider class")
    void registerCustomNameProvider_registersCustomProvider() throws IOException {
        IndexView index = buildIndex(EntityWithCustomNameProvider.class, CustomNameProvider.class);
        CollectingProducer producer = new CollectingProducer();
        MorphiumProcessor processor = new MorphiumProcessor();

        ClassInfo entityInfo = index.getClassByName(DotName.createSimple(EntityWithCustomNameProvider.class.getName()));
        AnnotationInstance entityAnnotation = entityInfo.declaredAnnotation(DotName.createSimple(Entity.class.getName()));

        processor.registerCustomNameProvider(entityAnnotation, producer);

        assertThat(producer.registeredClassNames)
                .as("the custom nameProvider class must be registered for reflection")
                .contains(CustomNameProvider.class.getName());
    }

    @Test
    @DisplayName("registerCustomNameProvider: does NOT re-register the default nameProvider (already registered unconditionally elsewhere)")
    void registerCustomNameProvider_skipsDefaultProvider() throws IOException {
        IndexView index = buildIndex(EntityWithDefaultNameProvider.class);
        CollectingProducer producer = new CollectingProducer();
        MorphiumProcessor processor = new MorphiumProcessor();

        ClassInfo entityInfo = index.getClassByName(DotName.createSimple(EntityWithDefaultNameProvider.class.getName()));
        AnnotationInstance entityAnnotation = entityInfo.declaredAnnotation(DotName.createSimple(Entity.class.getName()));

        processor.registerCustomNameProvider(entityAnnotation, producer);

        assertThat(producer.registeredClassNames)
                .as("DefaultNameProvider is already registered unconditionally by the caller; this method must not duplicate it")
                .doesNotContain(DefaultNameProvider.class.getName());
    }
}
