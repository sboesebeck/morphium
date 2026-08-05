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
package de.caluga.morphium.quarkus.migration;

import io.smallrye.config.WithDefault;

/**
 * Nested configuration interface for database migrations.
 *
 * <p>All properties live under the {@code quarkus.morphium.migration.*} prefix.
 *
 * <p>Example {@code application.properties}:
 * <pre>{@code
 * quarkus.morphium.migration.migrate-at-start=true
 * quarkus.morphium.migration.change-log-collection=morphiumChangeLog
 * quarkus.morphium.migration.lock-collection=morphiumMigrationLock
 * quarkus.morphium.migration.lock-ttl-seconds=60
 * }</pre>
 */
public interface MorphiumMigrationConfig {

    /**
     * Whether to run pending migrations automatically when the application starts.
     * Defaults to {@code false} — migrations must be triggered explicitly unless enabled.
     */
    @WithDefault("false")
    boolean migrateAtStart();

    /** Name of the MongoDB collection that tracks executed migrations. */
    @WithDefault("morphiumChangeLog")
    String changeLogCollection();

    /** Name of the MongoDB collection used for the distributed migration lock. */
    @WithDefault("morphiumMigrationLock")
    String lockCollection();

    /**
     * Time-to-live in seconds for the migration lock. Prevents deadlocks from crashed processes.
     * Must be greater than 0 and should exceed the maximum expected migration runtime.
     * If migrations take longer than this value, another instance may override the lock.
     */
    @WithDefault("60")
    int lockTtlSeconds();
}
