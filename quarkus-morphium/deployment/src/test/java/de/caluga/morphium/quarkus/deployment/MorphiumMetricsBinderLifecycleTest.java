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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.quarkus.MorphiumProducer;
import de.caluga.morphium.quarkus.observability.MorphiumMetricsBinder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Arc;
import io.quarkus.arc.ClientProxy;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.test.QuarkusUnitTest;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import jakarta.enterprise.event.Event;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof for a real, bytecode-verified finding from an upstream review (Stephan
 * Bösebeck, PR sboesebeck/morphium#332): {@code MorphiumProducer.buildMorphium()}/{@code onStop()}
 * previously wrapped {@code Arc.container().instance(MorphiumMetricsBinder.class)} in a
 * try-with-resources block. His bytecode reading of {@code AbstractInstanceHandle#destroy()}
 * (does the actual work) was correct. His conclusion that {@code InstanceHandle#close()}'s
 * default method (which decides WHETHER to call {@code destroy()}) invokes it for a
 * non-{@code @Dependent}-scoped bean needed one more level of bytecode reading to settle:
 * verified directly against {@code InstanceHandle.class} in arc-3.32.3.jar --
 * {@code close()}'s default implementation only calls {@code destroy()} when
 * {@code ArcContainer#strictCompatibility()} is {@code true} (default: {@code false}, and Quarkus'
 * own docs recommend leaving it {@code false}) OR the bean's scope IS {@code @Dependent}.
 * {@code MorphiumMetricsBinder} is {@code @ApplicationScoped} and this repo does not set
 * {@code quarkus.arc.strict-compatibility}, so in the actual default configuration the previous
 * try-with-resources code did NOT destroy the bean on every {@code buildMorphium()}/{@code onStop()}
 * call -- confirmed by first running this exact test against the pre-fix code (checked out from
 * commit 5701c3e16, before this PR's fix) and observing it pass identically to the post-fix code,
 * then confirming why via {@code InstanceHandle.class}'s bytecode and a same-instance/ClientProxy
 * check (see {@code clientProxyProtectsAgainstDestroyInDefaultConfiguration()}).
 * <p>
 * The fix in this PR (a lazily-resolved, reused {@code InstanceHandle} field, never wrapped in
 * try-with-resources except once at final shutdown) is kept regardless: it is still strictly more
 * correct (removes a latent dependency on {@code strictCompatibility()} staying {@code false}
 * forever, and on {@code @ApplicationScoped} never becoming {@code @Dependent}), and this test
 * class exists specifically to keep proving the observable contract -- meters survive multiple
 * connect/disconnect cycles with no duplicates and no leaks -- regardless of which of those two
 * mechanisms is doing the protecting on a given Quarkus/ArC version.
 * <p>
 * Runs two full connect/disconnect cycles against the same {@code MeterRegistry} and asserts
 * EXACTLY ONE registration per meter name after each connect (not just "the name is present
 * somewhere" -- a name-set check alone cannot distinguish one registration from a leaked
 * duplicate registered twice under the same MeterId) and zero morphium.* meters after each
 * shutdown. Uses a real Micrometer {@code SimpleMeterRegistry} (auto-created by {@code
 * quarkus-micrometer} when no registry extension like {@code quarkus-micrometer-registry-
 * prometheus} is present) via the InMemoryDriver, so no MongoDB is required.
 */
public class MorphiumMetricsBinderLifecycleTest {

    private static final List<String> EXPECTED_METER_NAMES = List.of(
        "morphium.driver.connections.pool",
        "morphium.driver.connections.in_use",
        "morphium.driver.connections.borrowed",
        "morphium.driver.connections.released",
        "morphium.driver.threads.waiting",
        "morphium.driver.errors",
        "morphium.driver.failovers",
        "morphium.cache.entries",
        "morphium.cache.hit_ratio",
        "morphium.write_buffer.entries"
    );

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
        .setArchiveProducer(new Supplier<JavaArchive>() {
            @Override
            public JavaArchive get() {
                return ShrinkWrap.create(JavaArchive.class)
                    .addClasses(LifecycleTestEntity.class);
            }
        })
        .overrideConfigKey("quarkus.morphium.driver-name", "InMemDriver")
        .overrideConfigKey("quarkus.morphium.hosts", "localhost:27017")
        .overrideConfigKey("quarkus.morphium.database", "morphium_metrics_lifecycle_test");

    @Entity(collectionName = "morphium_metrics_lifecycle_entity")
    public static class LifecycleTestEntity {
        @Id
        public MorphiumId id;
    }

    @Test
    @DisplayName("MorphiumMetricsBinder's registered meters survive buildMorphium()/onStop() unchanged, across two full cycles, with no duplicates")
    public void metersSurviveTwoFullConnectDisconnectCycles() {
        MorphiumProducer producer = Arc.container().instance(MorphiumProducer.class).get();
        MeterRegistry registry = Arc.container().instance(MeterRegistry.class).get();

        // Cycle 1: connect (this is the real CDI bean-creation path, not a direct method call --
        // it exercises the exact code path buildMorphium()'s InstanceHandle fix must not break).
        Morphium morphium1 = producer.morphium();
        assertThat(morphium1).as("first connect must succeed").isNotNull();
        assertExactlyOneRegistrationEach(registry, "after the first connect");

        // Simulate application shutdown via the real CDI event (not a direct onStop() call --
        // onStop() is package-private and this is the actual mechanism Quarkus uses).
        fireShutdownEvent();
        assertNoMorphiumMeters(registry, "after the first shutdown -- this is the exact "
            + "deregistration path the try-with-resources finding was concerned about");

        // Cycle 2: the producer's own `instance` field was reset to null by onStop(), so calling
        // morphium() again goes through the full buildMorphium() path again, including the fixed
        // InstanceHandle re-resolution.
        Morphium morphium2 = producer.morphium();
        assertThat(morphium2).as("second connect must succeed").isNotNull();
        assertThat(morphium2).as("second connect must build a fresh Morphium instance").isNotSameAs(morphium1);
        assertExactlyOneRegistrationEach(registry, "after the second connect -- no duplicates "
            + "left over from the first cycle's MorphiumMetricsBinder instance");

        fireShutdownEvent();
        assertNoMorphiumMeters(registry, "after the second shutdown");
    }

    /**
     * Documents WHY the pre-fix try-with-resources code did not actually destroy the bean in this
     * repo's default configuration -- see the class Javadoc for the full bytecode-verified
     * reasoning. {@code get()} on an {@code @ApplicationScoped} bean's {@code InstanceHandle}
     * returns a {@link ClientProxy}: the same proxy object on every lookup, which transparently
     * re-resolves the underlying contextual instance from the active context on each method call.
     * Even if {@code destroy()} genuinely ran (e.g. with {@code strictCompatibility=true}), code
     * holding only the proxy -- never true here, since {@code MorphiumMetricsBinder.registeredMeters}
     * is a plain instance field with no static/proxy-level survival -- would still not observe a
     * stale reference the way a raw instance reference would. This test exists to make that
     * mechanism explicit and regression-proof, not to argue the original finding was wrong to
     * raise: the underlying {@code destroy()} behavior IS real and IS scope/config-dependent, so
     * fixing the try-with-resources (done in this PR) remains the correct, forward-compatible
     * change regardless of what today's default configuration happens to paper over.
     */
    @Test
    @DisplayName("get() on MorphiumMetricsBinder's InstanceHandle returns a stable ClientProxy, not a raw instance reference")
    public void clientProxyProtectsAgainstDestroyInDefaultConfiguration() {
        Object firstLookup;
        try (InstanceHandle<MorphiumMetricsBinder> handle = Arc.container().instance(MorphiumMetricsBinder.class)) {
            firstLookup = handle.get();
        }
        Object secondLookup;
        try (InstanceHandle<MorphiumMetricsBinder> handle = Arc.container().instance(MorphiumMetricsBinder.class)) {
            secondLookup = handle.get();
        }

        assertThat(firstLookup)
            .as("get() must return a ClientProxy for an @ApplicationScoped bean, not the raw contextual instance")
            .isInstanceOf(ClientProxy.class);
        assertThat(firstLookup)
            .as("the proxy itself is stable across independent InstanceHandle lookups, even across "
                + "a try-with-resources close() in between -- this is what let the pre-fix code's "
                + "registeredMeters state survive in practice under this repo's default ArC "
                + "configuration (quarkus.arc.strict-compatibility left at its default false)")
            .isSameAs(secondLookup);
    }

    private static void fireShutdownEvent() {
        Event<ShutdownEvent> event = Arc.container().beanManager().getEvent().select(ShutdownEvent.class);
        event.fire(new ShutdownEvent());
    }

    /**
     * Asserts every expected morphium.* meter name is present EXACTLY once -- not merely
     * "present somewhere", which a plain name-set check cannot distinguish from a leaked
     * duplicate registered twice under the same MeterId (Micrometer's own
     * "This Gauge has been already registered" warning logs but does not fail on that case, so
     * only counting catches it).
     */
    private static void assertExactlyOneRegistrationEach(MeterRegistry registry, String when) {
        Map<String, Long> countsByName = morphiumMeterCountsByName(registry);
        for (String expectedName : EXPECTED_METER_NAMES) {
            assertThat(countsByName.getOrDefault(expectedName, 0L))
                .as("expected exactly one registration of '%s' %s, found %s", expectedName, when,
                    countsByName.getOrDefault(expectedName, 0L))
                .isEqualTo(1L);
        }
        assertThat(countsByName.keySet())
            .as("no unexpected morphium.* meters %s", when)
            .containsExactlyInAnyOrderElementsOf(EXPECTED_METER_NAMES);
    }

    private static void assertNoMorphiumMeters(MeterRegistry registry, String when) {
        assertThat(morphiumMeterCountsByName(registry).keySet())
            .as("no morphium.* meters must remain registered %s", when)
            .isEmpty();
    }

    private static Map<String, Long> morphiumMeterCountsByName(MeterRegistry registry) {
        return registry.getMeters().stream()
            .map(Meter::getId)
            .map(id -> id.getName())
            .filter(name -> name.startsWith("morphium."))
            .collect(Collectors.groupingBy(name -> name, Collectors.counting()));
    }
}
