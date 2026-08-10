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

import io.smallrye.config.WithDefault;

/**
 * Cache configuration group, nested under {@link MorphiumRuntimeConfig#cache()}.
 */
public interface CacheConfig {

    /** Global validity time for cached query results in milliseconds. */
    @WithDefault("60000")
    long globalValidTime();

    /** Whether query-result caching is enabled. */
    @WithDefault("true")
    boolean readCacheEnabled();
}
