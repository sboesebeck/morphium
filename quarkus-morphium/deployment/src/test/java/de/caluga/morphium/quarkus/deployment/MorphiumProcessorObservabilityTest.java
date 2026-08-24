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

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MorphiumProcessor#registerObservability}: verifies that
 * {@code MorphiumMetricsBinder} is registered as an additional CDI bean only when
 * {@code Capability.METRICS} (Micrometer) is present at build time, and is NOT registered
 * for an app that lacks it -- mirroring
 * {@code MorphiumIdJsonSerializationTest}/the Jackson/JSON-B Capability-gating pattern, but as
 * a plain unit test against the processor method directly (same style as
 * {@link MorphiumProcessorReflectionTest}), since {@code MorphiumProcessor} needs no live
 * Jandex index for this build step.
 */
@DisplayName("MorphiumProcessor#registerObservability — Capability.METRICS gating")
class MorphiumProcessorObservabilityTest {

    private static final String BINDER_CLASS = "de.caluga.morphium.quarkus.observability.MorphiumMetricsBinder";

    /** Collects every {@code AdditionalBeanBuildItem} produced, flattened to bean class names. */
    private static class CollectingBeanProducer implements BuildProducer<AdditionalBeanBuildItem> {
        final List<AdditionalBeanBuildItem> items = new ArrayList<>();

        @Override
        public void produce(AdditionalBeanBuildItem item) {
            items.add(item);
        }

        Set<String> beanClassNames() {
            Set<String> names = new HashSet<>();
            for (AdditionalBeanBuildItem item : items) {
                names.addAll(item.getBeanClasses());
            }
            return names;
        }
    }

    @Test
    @DisplayName("Capability.METRICS present -> MorphiumMetricsBinder IS registered as an additional bean")
    void metricsCapabilityPresent_registersBinder() {
        Capabilities capabilities = new Capabilities(Set.of(Capability.METRICS));
        CollectingBeanProducer producer = new CollectingBeanProducer();
        MorphiumProcessor processor = new MorphiumProcessor();

        processor.registerObservability(capabilities, producer);

        assertThat(producer.beanClassNames())
                .as("MorphiumMetricsBinder must be added as an AdditionalBeanBuildItem when Micrometer is present")
                .contains(BINDER_CLASS);
    }

    @Test
    @DisplayName("Capability.METRICS absent -> MorphiumMetricsBinder is NOT registered (no-op for apps without Micrometer)")
    void metricsCapabilityAbsent_doesNotRegisterBinder() {
        Capabilities capabilities = new Capabilities(Collections.emptySet());
        CollectingBeanProducer producer = new CollectingBeanProducer();
        MorphiumProcessor processor = new MorphiumProcessor();

        processor.registerObservability(capabilities, producer);

        assertThat(producer.items)
                .as("no AdditionalBeanBuildItem must be produced at all when Micrometer is absent")
                .isEmpty();
    }
}
