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
package de.caluga.morphium.showcase;

import de.caluga.morphium.showcase.security.JCacheService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the JCache (JSR-107) showcase feature: a second, standalone {@code Morphium}
 * instance backed by {@code MorphiumCacheJCacheImpl} genuinely caches query results (the
 * cache is populated after a query) and seeds/counts/resets independently of the CDI-injected
 * default-cache {@code Morphium} used everywhere else in this showcase.
 */
@QuarkusTest
class JCacheServiceTest {

    @Inject
    JCacheService jCacheService;

    @BeforeEach
    void setUp() {
        jCacheService.reset();
    }

    @Test
    void seedCreatesFiveReports() {
        jCacheService.seedIfEmpty();
        assertEquals(5, jCacheService.count());
    }

    @Test
    void seedIsIdempotentWhenNotEmpty() {
        jCacheService.seedIfEmpty();
        jCacheService.seedIfEmpty();
        assertEquals(5, jCacheService.count());
    }

    @Test
    void timedHitMissComparisonReturnsTwoNonNegativeTimings() {
        jCacheService.seedIfEmpty();

        long[] timings = jCacheService.timedHitMissComparison();

        assertEquals(2, timings.length);
        assertTrue(timings[0] >= 0);
        assertTrue(timings[1] >= 0);
    }

    @Test
    void cacheSizesArePopulatedAfterAQuery() {
        jCacheService.seedIfEmpty();
        jCacheService.timedHitMissComparison();

        var sizes = jCacheService.cacheSizes();

        assertFalse(sizes.isEmpty(), "the JCache-backed MorphiumCache should report at least one populated cache");
    }

    @Test
    void resetClearsReports() {
        jCacheService.seedIfEmpty();
        jCacheService.reset();
        assertEquals(0, jCacheService.count());
    }

    @Test
    void findAllReturnsSeededReports() {
        jCacheService.seedIfEmpty();
        assertEquals(5, jCacheService.findAll().size());
    }
}
