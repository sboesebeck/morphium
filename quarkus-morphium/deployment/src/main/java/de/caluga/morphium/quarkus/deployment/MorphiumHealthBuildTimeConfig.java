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

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Build-time configuration for Morphium health checks.
 *
 * <p>When enabled (the default), liveness, readiness and startup health checks
 * are registered with the SmallRye Health subsystem. Set to {@code false} to
 * suppress all Morphium health checks:
 * <pre>{@code
 * quarkus.morphium.health.enabled=false
 * }</pre>
 */
@ConfigMapping(prefix = "quarkus.morphium.health")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface MorphiumHealthBuildTimeConfig {

    /**
     * Whether Morphium health checks (liveness, readiness, startup) are enabled.
     */
    @WithDefault("true")
    boolean enabled();
}
