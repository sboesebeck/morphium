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
 * Configuration for how {@link java.time.LocalDateTime} values are stored in MongoDB.
 */
public interface LocalDateTimeConfig {

    /**
     * Whether to store {@link java.time.LocalDateTime} as a BSON Date ({@code ISODate})
     * instead of the Morphium-native {@code {sec: epochSecond, n: nanos}} Map format.
     *
     * <p>BSON Date format:
     * <ul>
     *   <li>Is compatible with data written by Morphia (legacy ORM)</li>
     *   <li>Enables native MongoDB date operations: sort, range queries, {@code $gt/$lt}</li>
     *   <li>Displays as human-readable ISO dates in mongosh and Atlas UI</li>
     * </ul>
     *
     * <p>Defaults to {@code true}. Set to {@code false} only if you need backward
     * compatibility with existing data written by Morphium in the Map format.
     */
    @WithDefault("true")
    boolean useBsonDate();
}
