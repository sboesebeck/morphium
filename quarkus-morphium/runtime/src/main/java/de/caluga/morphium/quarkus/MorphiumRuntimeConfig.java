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

import de.caluga.morphium.quarkus.migration.MorphiumMigrationConfig;
import de.caluga.morphium.quarkus.observability.MorphiumObservabilityConfig;

import java.util.Map;

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
 * quarkus.morphium.password=changeit
 * quarkus.morphium.max-connections=250
 * }</pre>
 */
@ConfigMapping(prefix = "quarkus.morphium")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface MorphiumRuntimeConfig extends MorphiumConnectionConfig {
    /** Nested LocalDateTime serialization configuration. */
    LocalDateTimeConfig localDateTime();

    /** Nested database migration configuration. */
    MorphiumMigrationConfig migration();

    /** Nested Micrometer observability configuration. Effective only when Micrometer is present. */
    MorphiumObservabilityConfig observability();

    /**
     * Named Morphium instances (MOR-032). Each name is a CDI qualifier: {@code @MorphiumInstance("analytics") Morphium analytics;} is the instance that is configured by
     * {@code quarkus.morphium.named."analytics".*}, with the same settings as the default instance ({@code hosts}, {@code database}, {@code username}, {@code driver-name},
     * {@code cache.*}, {@code ssl.*}, ...). The names are read from the configuration at build time, so a name cannot be added at run time. Migrations, Dev Services, metrics
     * and {@code @MorphiumTransactional} belong to the default instance only; the health checks cover every instance.
     */
    Map<String, NamedMorphiumConfig> named();
}
