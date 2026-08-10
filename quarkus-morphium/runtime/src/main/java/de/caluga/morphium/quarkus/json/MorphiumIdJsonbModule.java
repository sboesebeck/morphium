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
package de.caluga.morphium.quarkus.json;

import io.quarkus.jsonb.JsonbConfigCustomizer;

import jakarta.inject.Singleton;
import jakarta.json.bind.JsonbConfig;

/**
 * Registers {@link MorphiumIdJsonbAdapter} on the application's JSON-B
 * configuration so {@code MorphiumId} fields (de)serialize as a hex string —
 * the JSON-B counterpart of {@link MorphiumIdJacksonModule}.
 *
 * <p>This bean is registered automatically by the extension's build-time
 * processor when {@code quarkus-jsonb} is on the classpath. JSON-B is an
 * <em>optional</em> dependency of the extension, so this class is only loaded
 * when a JSON-B-based JSON layer is actually present.
 */
@Singleton
public class MorphiumIdJsonbModule implements JsonbConfigCustomizer {

    @Override
    public void customize(JsonbConfig config) {
        config.withAdapters(new MorphiumIdJsonbAdapter());
    }
}
