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
package de.caluga.morphium.quarkus.observability;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.MorphiumConfig;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The timer of the update operations against a real {@link Morphium}: the writer fires the events
 * that {@link MorphiumOperationMetrics} listens to. Runs on the InMemoryDriver; with
 * the environment variable {@code MORPHIUM_TEST_URI=mongodb://host:port/db} it runs against a real MongoDB.
 */
@DisplayName("MorphiumOperationMetrics with a real Morphium")
class MorphiumOperationMetricsWithMorphiumTest {

    @Entity
    public static class Counter {
        @Id
        public String id;
        public int hits;
        public List<String> tags;
    }

    private Morphium morphium;
    private SimpleMeterRegistry registry;
    private MorphiumMetricsBinder binder;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        String uri = System.getenv("MORPHIUM_TEST_URI");
        if (uri != null) {
            cfg.connectionSettings().setDatabase("mor031_" + System.nanoTime());
            cfg.clusterSettings().addHostToSeed(uri.replaceAll("^mongodb://", "").replaceAll("/.*$", ""));
            cfg.driverSettings().setDriverName("PooledDriver");
        } else {
            cfg.connectionSettings().setDatabase("mor031_" + System.nanoTime());
            cfg.driverSettings().setDriverName("InMemDriver");
        }
        morphium = new Morphium(cfg);
        registry = new SimpleMeterRegistry();
        binder = new MorphiumMetricsBinder();
        binder.registry = registry;
        binder.bindTo(morphium, cfg.connectionSettings().getDatabase(), true);
    }

    @AfterEach
    void tearDown() {
        binder.close();
        morphium.dropCollection(Counter.class);
        morphium.close();
    }

    private Timer timer(String type) {
        return registry.find("morphium.operations.duration").tag("update_type", type).tag("entity", "Counter").timer();
    }

    @Test
    @DisplayName("Query.inc, set and push appear as timers per update type")
    void theWriterPathIsTimed() {
        Counter c = new Counter();
        c.id = "c1";
        c.tags = new java.util.ArrayList<>();
        morphium.store(c);

        morphium.createQueryFor(Counter.class).f("id").eq("c1").inc("hits", 2);
        morphium.createQueryFor(Counter.class).f("id").eq("c1").inc("hits", 3);
        morphium.createQueryFor(Counter.class).f("id").eq("c1").set("hits", 10);
        morphium.createQueryFor(Counter.class).f("id").eq("c1").push("tags", "x");

        assertThat(timer("inc")).isNotNull();
        assertThat(timer("inc").count()).isEqualTo(2);
        assertThat(timer("set").count()).isEqualTo(1);
        assertThat(timer("push").count()).isEqualTo(1);
        assertThat(timer("inc").totalTime(java.util.concurrent.TimeUnit.NANOSECONDS)).isPositive();
        assertThat(morphium.createQueryFor(Counter.class).f("id").eq("c1").get().hits).isEqualTo(10);
    }

    @Test
    @DisplayName("after close the writes are no longer timed and the timers are gone")
    void closeStopsTheTimers() {
        Counter c = new Counter();
        c.id = "c2";
        morphium.store(c);
        morphium.createQueryFor(Counter.class).f("id").eq("c2").inc("hits", 1);
        assertThat(timer("inc")).isNotNull();

        binder.close();
        morphium.createQueryFor(Counter.class).f("id").eq("c2").inc("hits", 1);

        assertThat(timer("inc")).isNull();
    }
}
