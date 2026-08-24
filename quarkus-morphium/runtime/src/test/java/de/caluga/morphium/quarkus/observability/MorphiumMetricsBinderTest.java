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
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.driver.MorphiumDriver;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MorphiumMetricsBinder}: verifies that {@link MorphiumMetricsBinder#bindTo}
 * registers exactly the Phase 1 (MVP) gauge catalog from the observability plan (Section 5,
 * driver/cache/write-buffer rows only) against a real {@link io.micrometer.core.instrument.MeterRegistry},
 * that each gauge reads live from the underlying {@link Morphium}/{@link MorphiumDriver} stats maps
 * (not a snapshot taken at registration time -- this is exactly the bug the plan's cited
 * {@code MongoConnectionPoolMetrics} precedent hit via a WeakReference-GC'd {@code this}), and that
 * {@link MorphiumMetricsBinder#close()} deregisters them again (Section 6.4 hot-reload idempotency).
 *
 * <p>Uses a real {@link SimpleMeterRegistry} (no Quarkus container needed -- {@code registry} is a
 * plain field, set directly rather than via CDI injection) plus Mockito for {@link Morphium}/
 * {@link MorphiumDriver}, mirroring {@code MorphiumTransactionalInterceptorCommitRetryTest}'s
 * mock-driver style in the sibling {@code transaction} test package.
 */
@DisplayName("MorphiumMetricsBinder — Phase 1 MVP gauge registration")
class MorphiumMetricsBinderTest {

    private SimpleMeterRegistry registry;
    private MorphiumMetricsBinder binder;
    private Morphium morphium;
    private MorphiumDriver driver;
    private Map<MorphiumDriver.DriverStatsKey, Double> driverStats;
    private Map<String, Double> statistics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        binder = new MorphiumMetricsBinder();
        binder.registry = registry;

        driver = mock(MorphiumDriver.class);
        morphium = mock(Morphium.class);
        when(morphium.getDriver()).thenReturn(driver);

        driverStats = new HashMap<>();
        driverStats.put(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL, 5.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_USE, 2.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED, 42.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED, 40.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.THREADS_WAITING_FOR_CONNECTION, 0.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.ERRORS, 1.0);
        driverStats.put(MorphiumDriver.DriverStatsKey.FAILOVERS, 0.0);
        when(driver.getDriverStats()).thenReturn(driverStats);

        statistics = new HashMap<>();
        statistics.put(StatisticKeys.CHITSPERC.name(), 87.5);
        statistics.put(StatisticKeys.CACHE_ENTRIES.name(), 13.0);
        statistics.put(StatisticKeys.WRITE_BUFFER_ENTRIES.name(), 3.0);
        when(morphium.getStatistics()).thenReturn(statistics);
    }

    @Test
    @DisplayName("bindTo registers exactly the 10 MVP-scoped meters, tagged with database")
    void bindTo_registersAllMvpMeters() {
        binder.bindTo(morphium, "testdb");

        // Instantaneous values -- Gauges (Section 5 catalog: pool size, waiting threads,
        // cache/write-buffer levels).
        String[] expectedGaugeNames = {
                "morphium.driver.connections.pool",
                "morphium.driver.connections.in_use",
                "morphium.driver.threads.waiting",
                "morphium.cache.hit_ratio",
                "morphium.cache.entries",
                "morphium.write_buffer.entries",
        };
        for (String name : expectedGaugeNames) {
            Gauge gauge = registry.find(name).gauge();
            assertThat(gauge).as("gauge '%s' must be registered", name).isNotNull();
            assertThat(gauge.getId().getTag("database")).isEqualTo("testdb");
        }

        // Cumulative, monotonically-increasing values -- FunctionCounters (Section 5 catalog:
        // borrowed/released connections, errors, failovers), NOT Gauges: a Gauge on a cumulative
        // value breaks counter-oriented dashboards and rate()/increase() queries.
        String[] expectedCounterNames = {
                "morphium.driver.connections.borrowed",
                "morphium.driver.connections.released",
                "morphium.driver.errors",
                "morphium.driver.failovers",
        };
        for (String name : expectedCounterNames) {
            FunctionCounter counter = registry.find(name).functionCounter();
            assertThat(counter).as("counter '%s' must be registered as a FunctionCounter, not a Gauge", name).isNotNull();
            assertThat(counter.getId().getTag("database")).isEqualTo("testdb");
            assertThat(registry.find(name).gauge())
                    .as("'%s' must NOT also be registered as a Gauge", name)
                    .isNull();
        }

        // No Counter/Timer rows from MorphiumStorageListener/MorphiumTransactionEvent --
        // those are explicitly Phase 2, out of scope for this round.
        assertThat(registry.getMeters()).hasSize(expectedGaugeNames.length + expectedCounterNames.length);
    }

    @Test
    @DisplayName("gauges read live driver stats, not a snapshot taken at bindTo() time")
    void gauges_readLiveValues_notASnapshot() {
        binder.bindTo(morphium, "testdb");

        Gauge poolGauge = registry.find("morphium.driver.connections.pool").gauge();
        assertThat(poolGauge.value()).isEqualTo(5.0);

        // Mutate the underlying stats map (simulating the driver's pool changing size) and
        // read the gauge again -- without a live reference into Morphium this would still
        // report the stale 5.0, exactly the WeakReference/NaN bug the plan calls out.
        driverStats.put(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL, 9.0);
        assertThat(poolGauge.value()).isEqualTo(9.0);
    }

    @Test
    @DisplayName("gauges read live cache/write-buffer statistics too")
    void gauges_readLiveStatistics() {
        binder.bindTo(morphium, "testdb");

        Gauge cacheEntriesGauge = registry.find("morphium.cache.entries").gauge();
        assertThat(cacheEntriesGauge.value()).isEqualTo(13.0);

        statistics.put(StatisticKeys.CACHE_ENTRIES.name(), 21.0);
        assertThat(cacheEntriesGauge.value()).isEqualTo(21.0);
    }

    @Test
    @DisplayName("close() deregisters every gauge previously registered by this binder")
    void close_deregistersAllGauges() {
        binder.bindTo(morphium, "testdb");
        assertThat(registry.getMeters()).isNotEmpty();

        binder.close();

        assertThat(registry.getMeters())
                .as("all Meters registered by bindTo() must be removed by close()")
                .isEmpty();
    }

    @Test
    @DisplayName("hot-reload: close() then bindTo() again leaves exactly one set of gauges, no duplicates")
    void closeThenRebind_doesNotLeaveDuplicateGauges() {
        binder.bindTo(morphium, "testdb");
        int firstCount = registry.getMeters().size();

        // Simulate a dev-mode hot-reload: MorphiumProducer.buildMorphium() calls close() before
        // re-binding against the freshly rebuilt Morphium instance (Section 6.4).
        binder.close();

        Morphium reloadedMorphium = mock(Morphium.class);
        MorphiumDriver reloadedDriver = mock(MorphiumDriver.class);
        when(reloadedMorphium.getDriver()).thenReturn(reloadedDriver);
        when(reloadedDriver.getDriverStats()).thenReturn(driverStats);
        when(reloadedMorphium.getStatistics()).thenReturn(statistics);

        binder.bindTo(reloadedMorphium, "testdb");

        assertThat(registry.getMeters()).hasSize(firstCount);
    }
}
