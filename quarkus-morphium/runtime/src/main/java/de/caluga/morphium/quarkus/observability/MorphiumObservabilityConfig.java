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
package de.caluga.morphium.quarkus.observability;

import io.smallrye.config.WithDefault;

/**
 * Nested configuration interface for the optional Micrometer observability module.
 *
 * <p>All properties live under the {@code quarkus.morphium.observability.*} prefix. This entire
 * interface only has an effect when Micrometer is on the application's classpath ({@code
 * Capability.METRICS} present at build time — see {@code MorphiumProcessor#registerObservability}):
 * for an app without Micrometer, {@code MorphiumMetricsBinder} is never registered as a bean at
 * all, so these properties are read by nothing and have no effect regardless of their value.
 *
 * <p>Example {@code application.properties}:
 * <pre>{@code
 * quarkus.morphium.observability.enabled=false
 * }</pre>
 *
 * <p><b>Naming note:</b> the observability-module-plan.md (Section 4.1) names this class
 * {@code MorphiumObservabilityRuntimeConfig} and describes it with a standalone
 * {@code @ConfigMapping(prefix="quarkus.morphium.observability")}. Implemented instead as a
 * nested config interface (this shape) referenced from {@code MorphiumRuntimeConfig.observability()},
 * mirroring the module's own existing {@code MorphiumMigrationConfig} precedent
 * (quarkus.morphium.migration.*) rather than introducing a second, structurally different,
 * top-level {@code @ConfigMapping} for the same {@code quarkus.morphium.*} property tree. Same
 * property path and defaults the plan specifies; different Java-level composition.
 *
 * <p><b>Phase 1 (MVP) scope note:</b> only {@link #enabled()} is implemented in this phase. The
 * plan's Section 7 also specifies {@code poll-interval}, {@code per-host-connections}, and
 * {@code include-storage-listener-metrics} — those govern behaviour (per-host connection tags,
 * the Counter/Timer metrics sourced from {@code MorphiumStorageListener}/
 * {@code MorphiumTransactionEvent}) that Phase 1 does not implement yet (deferred to a later
 * phase per the plan's Section 4.1/8), so adding those properties now would document
 * configuration knobs with nothing behind them. They will be added alongside the phase that
 * implements the behaviour they control.
 */
public interface MorphiumObservabilityConfig {

    /**
     * Runtime kill-switch for the observability module. Defaults to {@code true} — but only takes
     * effect at all when {@code Capability.METRICS} was already detected at build time; an app
     * without Micrometer sees no behaviour change regardless of this value, since the binder bean
     * was never registered in the first place. Lets an app that has Micrometer on its classpath
     * for an unrelated reason (e.g. a different extension's transitive dependency) opt out of
     * Morphium's gauges specifically, without removing Micrometer entirely.
     */
    @WithDefault("true")
    boolean enabled();
}
