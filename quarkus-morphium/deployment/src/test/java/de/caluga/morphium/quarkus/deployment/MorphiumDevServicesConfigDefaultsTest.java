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
package de.caluga.morphium.quarkus.deployment;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link MorphiumDevServicesBuildTimeConfig} {@code @ConfigMapping}.
 *
 * <p>Uses SmallRye Config directly (no Quarkus container required) to verify that
 * all {@code @WithDefault} values are set correctly and that property-name mapping
 * (e.g. {@code replica-set} → {@code replicaSet()}) works as expected.
 */
@DisplayName("MorphiumDevServicesBuildTimeConfig – defaults and overrides")
class MorphiumDevServicesConfigDefaultsTest {

    // -------------------------------------------------------------------------
    // Default values – no explicit config source, only @WithDefault applies
    // -------------------------------------------------------------------------

    private static MorphiumDevServicesBuildTimeConfig defaults;

    @BeforeAll
    static void buildDefaultConfig() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .build();
        defaults = sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
    }

    @Test
    @DisplayName("enabled() defaults to true")
    void enabled_defaultsToTrue() {
        assertThat(defaults.enabled()).isTrue();
    }

    @Test
    @DisplayName("imageName() defaults to 'mongo:8'")
    void imageName_defaultsToMongo8() {
        assertThat(defaults.imageName()).isEqualTo("mongo:8");
    }

    @Test
    @DisplayName("databaseName() defaults to 'morphium-dev'")
    void databaseName_defaultsToMorphiumDev() {
        assertThat(defaults.databaseName()).isEqualTo("morphium-dev");
    }

    @Test
    @DisplayName("replicaSet() defaults to true")
    void replicaSet_defaultsToTrue() {
        assertThat(defaults.replicaSet()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Overrides – verify property-name mapping and value parsing
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("replica-set property maps to replicaSet() and accepts 'false'")
    void replicaSet_canBeDisabledViaProperty() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .withDefaultValue("quarkus.morphium.devservices.replica-set", "false")
                .build();
        MorphiumDevServicesBuildTimeConfig cfg =
                sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
        assertThat(cfg.replicaSet()).isFalse();
    }

    @Test
    @DisplayName("enabled property can be set to false")
    void enabled_canBeDisabled() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .withDefaultValue("quarkus.morphium.devservices.enabled", "false")
                .build();
        MorphiumDevServicesBuildTimeConfig cfg =
                sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
        assertThat(cfg.enabled()).isFalse();
    }

    @Test
    @DisplayName("imageName can be overridden to a custom image")
    void imageName_canBeOverridden() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .withDefaultValue("quarkus.morphium.devservices.image-name", "mongo:7")
                .build();
        MorphiumDevServicesBuildTimeConfig cfg =
                sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
        assertThat(cfg.imageName()).isEqualTo("mongo:7");
    }

    @Test
    @DisplayName("databaseName can be overridden")
    void databaseName_canBeOverridden() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .withDefaultValue("quarkus.morphium.devservices.database-name", "my-dev-db")
                .build();
        MorphiumDevServicesBuildTimeConfig cfg =
                sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
        assertThat(cfg.databaseName()).isEqualTo("my-dev-db");
    }

    @Test
    @DisplayName("replica-set=true is idempotent (same as default)")
    void replicaSet_trueIsIdempotent() {
        SmallRyeConfig sr = new SmallRyeConfigBuilder()
                .withMapping(MorphiumDevServicesBuildTimeConfig.class)
                .withDefaultValue("quarkus.morphium.devservices.replica-set", "true")
                .build();
        MorphiumDevServicesBuildTimeConfig cfg =
                sr.getConfigMapping(MorphiumDevServicesBuildTimeConfig.class);
        assertThat(cfg.replicaSet()).isTrue();
    }
}
