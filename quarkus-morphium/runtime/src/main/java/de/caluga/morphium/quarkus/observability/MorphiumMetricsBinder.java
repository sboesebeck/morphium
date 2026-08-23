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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Registers Micrometer {@link Gauge}s (for instantaneous values -- pool size, waiting threads,
 * cache/write-buffer levels) and {@link FunctionCounter}s (for cumulative, monotonically
 * increasing values -- borrowed/released connections, errors, failovers; see the observability
 * plan's Section 5 metric catalog for which of Morphium's connection-pool/driver statistics
 * ({@link MorphiumDriver#getDriverStats()}) and cache/write-buffer statistics
 * ({@link Morphium#getStatistics()}) is which meter type).
 *
 * <p>This bean only exists on the classpath/in the CDI container when
 * {@code Capability.METRICS} was present at build time (see
 * {@code MorphiumProcessor#registerObservability}) — it must never be referenced from a
 * code path that runs when Micrometer is absent.
 *
 * <p><b>Registration timing (Section 6.1 of the observability plan):</b> {@link #bindTo(Morphium, String)}
 * must only be called from {@code MorphiumProducer#buildMorphium()}, after the {@link Morphium}
 * instance has already been constructed and connected — never from an early CDI
 * {@code @Observes StartupEvent} observer that would dereference {@code Instance<Morphium>} (and
 * thus trigger the lazy connect prematurely). This class itself does not observe any startup
 * event and does not inject {@code Morphium}; it is purely a passive binder invoked explicitly,
 * once {@code m} already exists.
 *
 * <p><b>Hot-reload idempotency (Section 6.4):</b> every {@link Meter} this binder registers is
 * kept in {@link #registeredMeters} so {@link #close()} can remove them again. This must be
 * called before a subsequent {@link #bindTo(Morphium, String)} (or the same effect: on shutdown), or a
 * dev-mode hot-reload will otherwise leave the previous instance's gauges registered against a
 * stale {@link Morphium} reference. Micrometer's {@link Gauge} implementation holds the {@code m}
 * passed to {@code bindTo} only via a {@link java.lang.ref.WeakReference} internally (see
 * {@code io.micrometer.core.instrument.internal.DefaultGauge}) -- this binder itself never keeps
 * a strong reference to {@code m}, only {@link Meter.Id}s in {@link #registeredMeters}. What
 * actually prevents the WeakReference-GC'd-to-NaN bug the plan cites from the hand-written
 * {@code MongoConnectionPoolMetrics} precedent is that {@code MorphiumProducer} itself holds the
 * same {@link Morphium} instance strongly for the application's lifetime (its {@code instance}
 * field, populated by {@code buildMorphium()}) -- not anything this binder bean does on its own.
 */
@ApplicationScoped
public class MorphiumMetricsBinder {

    private static final Logger log = LoggerFactory.getLogger(MorphiumMetricsBinder.class);

    static final String DATABASE_TAG = "database";

    @Inject
    MeterRegistry registry;

    private final List<Meter.Id> registeredMeters = new ArrayList<>();

    /**
     * Registers all in-scope gauges against the given, already-connected {@link Morphium}
     * instance. Idempotent with respect to prior registrations on this bean: callers on a
     * hot-reload path must call {@link #close()} first (see class Javadoc).
     *
     * @param m        the already-connected Morphium instance to gauge
     * @param database the configured database name (from {@code MorphiumRuntimeConfig#database()},
     *                 the same value {@code MorphiumProducer.buildMorphium()} uses to configure the
     *                 connection), used as the {@code database} tag per the metric catalog. Passed
     *                 explicitly rather than read back via {@code Morphium.getConfig().getDatabase()}
     *                 (deprecated in {@code MorphiumConfig}).
     */
    public synchronized void bindTo(Morphium m, String database) {
        Tags tags = Tags.of(DATABASE_TAG, database);

        registerDriverGauge(m, tags, "morphium.driver.connections.pool",
                MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL);
        registerDriverGauge(m, tags, "morphium.driver.connections.in_use",
                MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_USE);
        registerDriverCounter(m, tags, "morphium.driver.connections.borrowed",
                MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED);
        registerDriverCounter(m, tags, "morphium.driver.connections.released",
                MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED);
        registerDriverGauge(m, tags, "morphium.driver.threads.waiting",
                MorphiumDriver.DriverStatsKey.THREADS_WAITING_FOR_CONNECTION);
        registerDriverCounter(m, tags, "morphium.driver.errors",
                MorphiumDriver.DriverStatsKey.ERRORS);
        registerDriverCounter(m, tags, "morphium.driver.failovers",
                MorphiumDriver.DriverStatsKey.FAILOVERS);

        registerStatisticGauge(m, tags, "morphium.cache.hit_ratio", StatisticKeys.CHITSPERC);
        registerStatisticGauge(m, tags, "morphium.cache.entries", StatisticKeys.CACHE_ENTRIES);
        registerStatisticGauge(m, tags, "morphium.write_buffer.entries", StatisticKeys.WRITE_BUFFER_ENTRIES);

        log.debug("Morphium: registered {} Micrometer meters for database '{}'",
                registeredMeters.size(), database);
    }

    private void registerDriverGauge(Morphium m, Tags tags, String name, MorphiumDriver.DriverStatsKey key) {
        Gauge gauge = Gauge.builder(name, m, target -> readDriverStat(target, key))
                .tags(tags)
                .register(registry);
        registeredMeters.add(gauge.getId());
    }

    /**
     * Registers a cumulative, monotonically-increasing driver value ({@link MorphiumDriver}
     * itself already counts it as a running total -- see the observability plan's Section 5
     * metric catalog, which classifies {@code connections.borrowed}/{@code connections.released}/
     * {@code errors}/{@code failovers} as Counter rows, not Gauge rows) as a Micrometer
     * {@link FunctionCounter} rather than a {@link Gauge}. A Gauge on a cumulative value publishes
     * gauge metadata to the backend, which breaks counter-oriented dashboards, {@code rate()}/
     * {@code increase()} queries, and reset-on-restart handling that assume genuine counter
     * semantics -- the underlying value read is identical to {@link #registerDriverGauge}, only
     * the Micrometer meter type differs.
     */
    private void registerDriverCounter(Morphium m, Tags tags, String name, MorphiumDriver.DriverStatsKey key) {
        FunctionCounter counter = FunctionCounter.builder(name, m, target -> readDriverStat(target, key))
                .tags(tags)
                .register(registry);
        registeredMeters.add(counter.getId());
    }

    private void registerStatisticGauge(Morphium m, Tags tags, String name, StatisticKeys key) {
        Gauge gauge = Gauge.builder(name, m, target -> readStatistic(target, key))
                .tags(tags)
                .register(registry);
        registeredMeters.add(gauge.getId());
    }

    private static double readDriverStat(Morphium m, MorphiumDriver.DriverStatsKey key) {
        Map<MorphiumDriver.DriverStatsKey, Double> stats = m.getDriver().getDriverStats();
        Double value = stats.get(key);
        return value == null ? 0.0 : value;
    }

    private static double readStatistic(Morphium m, StatisticKeys key) {
        Map<String, Double> stats = m.getStatistics();
        Double value = stats.get(key.name());
        return value == null ? 0.0 : value;
    }

    /**
     * Deregisters every {@link Meter} this binder has registered so far. Called from
     * {@code MorphiumProducer#onStop()} alongside {@code instance.close()}, and must also be
     * called before a subsequent {@link #bindTo(Morphium, String)} on a dev-mode hot-reload to avoid
     * leaving stale gauges referencing a superseded {@link Morphium} instance registered
     * (Section 6.4 of the observability plan).
     */
    public synchronized void close() {
        for (Meter.Id id : registeredMeters) {
            registry.remove(id);
        }
        registeredMeters.clear();
    }
}
