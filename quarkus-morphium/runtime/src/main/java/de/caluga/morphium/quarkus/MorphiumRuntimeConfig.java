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
package de.caluga.morphium.quarkus;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import de.caluga.morphium.quarkus.migration.MorphiumMigrationConfig;

import java.util.List;
import java.util.Optional;

/**
 * Type-safe runtime configuration for the Morphium extension.
 *
 * <p>All properties are resolved from {@code application.properties} at
 * startup – no reflection, no Unsafe access, purely CDI/SmallRye Config.
 *
 * <p>Example {@code application.properties}:
 * <pre>{@code
 * quarkus.morphium.database=my-app-db
 * quarkus.morphium.hosts=mongo1:27017,mongo2:27017
 * quarkus.morphium.username=admin
 * quarkus.morphium.password=secret
 * quarkus.morphium.max-connections=250
 * }</pre>
 */
@ConfigMapping(prefix = "quarkus.morphium")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface MorphiumRuntimeConfig {

    /**
     * MongoDB host list in {@code host:port} format.
     * Multiple hosts are separated by commas in application.properties.
     */
    @WithDefault("localhost:27017")
    List<String> hosts();

    /** MongoDB database name. */
    String database();

    /** MongoDB username (optional). */
    Optional<String> username();

    /** MongoDB password (optional). */
    Optional<String> password();

    /** Authentication database, defaults to {@code admin}. */
    @WithDefault("admin")
    String authDatabase();

    /**
     * Read preference for MongoDB queries.
     * Accepted values: {@code primary}, {@code primaryPreferred},
     * {@code secondary}, {@code secondaryPreferred}, {@code nearest}.
     */
    @WithDefault("primary")
    String readPreference();

    /**
     * Index creation strategy. Controls when and if Morphium ensures that
     * {@code @Index} annotations are reflected as actual MongoDB indexes.
     *
     * <ul>
     *   <li>{@code create-on-startup} – <b>(default)</b> create missing indexes
     *       when the Morphium instance connects. Reliable even when the first
     *       write happens inside a transaction.</li>
     *   <li>{@code warn-on-startup} – log a warning for every missing index at
     *       startup but do <em>not</em> create them.</li>
     *   <li>{@code create-on-write-new-col} – create indexes lazily, only when
     *       writing to a collection that does not yet exist (skipped inside
     *       transactions).</li>
     *   <li>{@code no-check} – disable all index management.</li>
     * </ul>
     */
    @WithDefault("create-on-startup")
    IndexCheckMode indexCheck();

    /** Strategy for automatic index management. */
    enum IndexCheckMode {
        /** Do not check or create indexes. */
        NO_CHECK,
        /** Log warnings for missing indexes at startup. */
        WARN_ON_STARTUP,
        /** Create missing indexes at startup (recommended). */
        CREATE_ON_STARTUP,
        /** Create indexes only when writing to a new collection (not inside transactions). */
        CREATE_ON_WRITE_NEW_COL
    }

    /** Maximum number of MongoDB connections in the pool. */
    @WithDefault("250")
    int maxConnections();

    /**
     * Maximum time (in milliseconds) for low-level operations such as waiting
     * for a connection from the pool, driver-level timeouts, and change streams.
     *
     * <p>This does <em>not</em> affect query execution time limits — use
     * {@link #defaultQueryTimeoutMs()} for that.
     */
    @WithDefault("2000")
    int maxWaitTime();

    /**
     * Default server-side time limit (in milliseconds) for queries when no
     * per-query {@code maxTimeMS} is set via {@code Query.setMaxTimeMS()}.
     *
     * <p>MongoDB enforces this as {@code maxTimeMS} across the entire cursor
     * lifecycle (initial {@code find} + all subsequent {@code getMore} operations).
     * If a query exceeds this limit, MongoDB returns error 50 ({@code ExceededTimeLimit}).
     *
     * <p>Set to {@code 0} (the default) to disable the server-side time limit
     * entirely (Morphium will set {@code noCursorTimeout} instead). Set to a
     * positive value (e.g. {@code 60000}) to enforce a global query timeout.
     */
    @WithDefault("0")
    int defaultQueryTimeoutMs();

    /**
     * Optional MongoDB Atlas connection string ({@code mongodb+srv://...}).
     * When present this overrides {@link #hosts()}.
     */
    Optional<String> atlasUrl();

    /**
     * Morphium driver name. Defaults to {@code PooledDriver}.
     * Use {@code InMemDriver} for tests (no MongoDB required).
     */
    @WithDefault("PooledDriver")
    String driverName();

    /**
     * MongoDB replica set name. When set, Morphium connects in replica set mode
     * which is required for transactions. Dev Services sets this automatically
     * when {@code quarkus.morphium.devservices.replica-set=true}.
     */
    Optional<String> replicaSetName();

    /**
     * Number of connection attempts before giving up (minimum {@code 1}).
     * Useful in CI environments (Docker-in-Docker) where the MongoDB replica set
     * primary may not be immediately reachable after the container starts.
     * Set to {@code 1} to disable retries. Values below 1 are treated as 1.
     */
    @WithDefault("5")
    int connectRetries();

    /** Nested cache configuration. */
    CacheConfig cache();

    /** Nested TLS / X.509 configuration. */
    SslConfig ssl();

    /** Nested LocalDateTime serialization configuration. */
    LocalDateTimeConfig localDateTime();

    /** Nested database migration configuration. */
    MorphiumMigrationConfig migration();
}
