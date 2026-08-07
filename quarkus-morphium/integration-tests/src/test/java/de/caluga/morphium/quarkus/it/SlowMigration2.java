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
 * Second slow test migration used by {@code MorphiumMigrationTest}'s lock-renewal regression
 * tests. Depending on the test it is used two different ways:
 * <ul>
 *   <li>Run directly after {@link SlowMigration} (at a short sleep) to prove that
 *       {@code renewLock()} renews the lock BETWEEN change units.</li>
 *   <li>Run alone, with {@link #SLEEP_MS} temporarily raised well above the test's lock TTL, to
 *       prove that the in-flight heartbeat renews the lock WHILE a single unit is still
 *       executing.</li>
 * </ul>
 *
 * <p>{@link #SLEEP_MS} is intentionally mutable (not {@code final}) so the second test case can
 * raise it for the duration of that one test and restore it afterwards, instead of needing a
 * third near-duplicate migration class just to get a different sleep duration.
 */
@MorphiumChangeUnit(id = "901-slow2", order = "901", author = "test")
public class SlowMigration2 {

    /**
     * How long {@link #execute} sleeps, in milliseconds. Mutable so tests can temporarily
     * override it; callers that do so MUST restore the original value afterwards (e.g. in a
     * {@code finally} block) since this is shared, static state.
     */
    public static volatile long SLEEP_MS = 1500L;

    @Execution
    public void execute(Morphium morphium) throws InterruptedException {
        Thread.sleep(SLEEP_MS);
    }
}
