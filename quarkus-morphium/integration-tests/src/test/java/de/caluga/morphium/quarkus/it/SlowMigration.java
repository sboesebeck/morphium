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
 * Test migration that sleeps for longer than the short lock TTL used by
 * {@code MorphiumMigrationTest}'s lock-renewal regression test, to prove that
 * {@code MorphiumMigrationRunner} renews the lock's {@code expires_at} between migrations
 * instead of leaving it to expire mid-run.
 */
@MorphiumChangeUnit(id = "900-slow", order = "900", author = "test")
public class SlowMigration {

    /** How long {@link #execute} sleeps, in milliseconds. Longer than the test's lock TTL. */
    public static final long SLEEP_MS = 1500L;

    @Execution
    public void execute(Morphium morphium) throws InterruptedException {
        Thread.sleep(SLEEP_MS);
    }
}
