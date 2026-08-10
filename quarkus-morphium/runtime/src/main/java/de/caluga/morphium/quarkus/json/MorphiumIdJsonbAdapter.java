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

import de.caluga.morphium.driver.MorphiumId;

import jakarta.json.bind.adapter.JsonbAdapter;

/**
 * JSON-B equivalent of {@link MorphiumIdJacksonModule}: maps {@link MorphiumId}
 * to and from its canonical 24-character hex string so that REST endpoints using
 * the JSON-B serialization layer ({@code quarkus-resteasy-jsonb} /
 * {@code quarkus-rest-jsonb}) emit {@code "id":"<hex>"} instead of the internal
 * {@code {pid, counter, machineId, bytes, time}} struct.
 *
 * @see MorphiumIdJacksonModule for the rationale and the production bug this fixes
 */
public class MorphiumIdJsonbAdapter implements JsonbAdapter<MorphiumId, String> {

    @Override
    public String adaptToJson(MorphiumId id) {
        return id == null ? null : id.toString();
    }

    @Override
    public MorphiumId adaptFromJson(String hex) {
        return hex == null || hex.isBlank() ? null : new MorphiumId(hex);
    }
}
