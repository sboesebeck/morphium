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
import de.caluga.morphium.quarkus.migration.Execution;
import de.caluga.morphium.quarkus.migration.MorphiumChangeUnit;

/**
 * Test migration used by {@code MorphiumMigrationTest}'s lock-renewal regression tests to prove
 * that {@code MorphiumMigrationRunner} renews the lock's {@code expires_at} BETWEEN migrations
 * (via {@code renewLock()} in the {@code execute()} loop) instead of leaving it to expire
 * mid-run.
 *
 * <p>{@link #SLEEP_MS} is mutable (not {@code final}) so the test can temporarily set a short
 * sleep well below the in-flight heartbeat's tick interval -- isolating the between-units
 * renewal mechanism from the separate in-flight heartbeat, which is covered by its own,
 * dedicated test. Callers that override it MUST restore the original value afterwards (e.g. in
 * a {@code finally} block) since this is shared, static state.
 */
@MorphiumChangeUnit(id = "900-slow", order = "900", author = "test")
public class SlowMigration {

    /** How long {@link #execute} sleeps, in milliseconds. */
    public static volatile long SLEEP_MS = 1500L;

    @Execution
    public void execute(Morphium morphium) throws InterruptedException {
        Thread.sleep(SLEEP_MS);
    }
}
