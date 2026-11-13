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
package de.caluga.morphium.showcase.security;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.cache.MorphiumCacheJCacheImpl;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.showcase.security.entity.JCacheReport;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import java.util.List;
import java.util.Map;

/**
 * Service layer demonstrating Morphium's <strong>JCache (JSR-107) cache integration</strong>
 * via {@code MorphiumCacheJCacheImpl}.
 *
 * <h3>Why a Second, Standalone {@code Morphium} Instance</h3>
 * <p>The CDI-injected {@code Morphium} bean used everywhere else in this showcase is wired by
 * {@code MorphiumProducer} with the <em>default</em> {@code MorphiumCacheImpl}. Switching the
 * cache implementation is a {@code MorphiumConfig.cacheSettings().setCache(...)} choice made
 * once, before the instance connects -- it cannot be swapped on an already-running instance.
 * Rather than reconfiguring the shared instance (which would affect every other feature in
 * this showcase), this service constructs its own independent {@code Morphium}, backed by
 * {@code InMemDriver} against a separate database name, with
 * {@code MorphiumCacheJCacheImpl} as its cache.</p>
 *
 * <h3>Why {@code @Startup}</h3>
 * <p>{@code @ApplicationScoped} beans are created lazily by default -- only on first actual
 * use. Without {@code @Startup}, if nobody ever visits {@code /security} in a given run,
 * {@code jcacheMorphium} is never constructed; the {@code @Observes ShutdownEvent} method
 * below would then trigger its lazy creation as a side effect of merely being invoked during
 * shutdown, calling {@code new Morphium(cfg)} -- which registers a JVM shutdown hook -- while
 * shutdown is already in progress ({@code IllegalStateException: Shutdown in progress}).
 * {@code @Startup} forces eager creation when the application starts, so the instance always
 * exists (and its close() call during shutdown is always meaningful) regardless of whether the
 * security demo page was ever visited.</p>
 */
@ApplicationScoped
@Startup
public class JCacheService {

    private Morphium jcacheMorphium;

    @PostConstruct
    void init() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("showcase-jcache-demo");
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        cfg.cacheSettings().setCache(new MorphiumCacheJCacheImpl());
        jcacheMorphium = new Morphium(cfg);
    }

    void onShutdown(@Observes ShutdownEvent event) {
        if (jcacheMorphium != null) {
            jcacheMorphium.close();
        }
    }

    /** Seeds five reports if the collection is empty. */
    public void seedIfEmpty() {
        if (jcacheMorphium.createQueryFor(JCacheReport.class).countAll() == 0) {
            for (int i = 1; i <= 5; i++) {
                jcacheMorphium.store(JCacheReport.builder()
                        .title("Report " + i)
                        .revenue(1000.0 * i)
                        .build());
            }
        }
    }

    /**
     * Runs the same query twice and times both. The first run is a cache miss (real MongoDB
     * round-trip, or here the InMemDriver's equivalent in-memory lookup); the second is served
     * from the JCache-backed result cache and should be measurably faster with zero additional
     * "query" work behind it.
     *
     * @return a two-element list: [missElapsedNanos, hitElapsedNanos]
     */
    public long[] timedHitMissComparison() {
        // Clear first so this comparison is not accidentally already warm from a previous call.
        jcacheMorphium.getCache().clearCachefor(JCacheReport.class);

        long t0 = System.nanoTime();
        jcacheMorphium.createQueryFor(JCacheReport.class).asList();
        long miss = System.nanoTime() - t0;

        long t1 = System.nanoTime();
        jcacheMorphium.createQueryFor(JCacheReport.class).asList();
        long hit = System.nanoTime() - t1;

        return new long[] {miss, hit};
    }

    public List<JCacheReport> findAll() {
        return jcacheMorphium.createQueryFor(JCacheReport.class).asList();
    }

    public long count() {
        return jcacheMorphium.createQueryFor(JCacheReport.class).countAll();
    }

    /** Per-cache-name entry counts (id cache and result cache for this entity type). */
    public Map<String, Integer> cacheSizes() {
        return jcacheMorphium.getCache().getSizes();
    }

    public void reset() {
        jcacheMorphium.dropCollection(JCacheReport.class);
    }
}
