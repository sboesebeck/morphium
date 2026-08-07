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

import static org.assertj.core.api.Assertions.assertThat;

import de.caluga.morphium.driver.MorphiumId;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link MorphiumIdJsonbModule} / {@link MorphiumIdJsonbAdapter}
 * map {@link MorphiumId} to and from a flat hex string under JSON-B.
 */
@DisplayName("MorphiumId JSON-B (de)serialization")
class MorphiumIdJsonbAdapterTest {

    private Jsonb jsonbWithAdapter() {
        JsonbConfig config = new JsonbConfig();
        new MorphiumIdJsonbModule().customize(config);
        return JsonbBuilder.create(config);
    }

    /** A minimal entity-shaped DTO with an {@code @Id}-style MorphiumId field. */
    public static class Doc {
        public MorphiumId id;
        public String name;
    }

    @Test
    @DisplayName("entity serializes id as \"<hex>\", not the {pid,counter,...} struct")
    void serializesAsHexString() throws Exception {
        MorphiumId id = new MorphiumId();
        Doc doc = new Doc();
        doc.id = id;
        doc.name = "widget";

        try (Jsonb jsonb = jsonbWithAdapter()) {
            String json = jsonb.toJson(doc);

            assertThat(json).contains("\"id\":\"" + id + "\"");
            assertThat(json).doesNotContain("pid");
            assertThat(json).doesNotContain("counter");
            assertThat(json).doesNotContain("machineId");
            assertThat(json).doesNotContain("bytes");
        }
    }

    @Test
    @DisplayName("\"<hex>\" deserializes back into an equal MorphiumId")
    void deserializesFromHexString() throws Exception {
        MorphiumId id = new MorphiumId();

        try (Jsonb jsonb = jsonbWithAdapter()) {
            String json = "{\"id\":\"" + id + "\",\"name\":\"widget\"}";
            Doc parsed = jsonb.fromJson(json, Doc.class);

            assertThat(parsed.id).isEqualTo(id);
            assertThat(parsed.name).isEqualTo("widget");
        }
    }

    @Test
    @DisplayName("round-trips entity -> JSON -> entity preserving id identity")
    void roundTrips() throws Exception {
        MorphiumId id = new MorphiumId();
        Doc doc = new Doc();
        doc.id = id;
        doc.name = "round";

        try (Jsonb jsonb = jsonbWithAdapter()) {
            Doc back = jsonb.fromJson(jsonb.toJson(doc), Doc.class);
            assertThat(back.id).isEqualTo(id);
        }
    }
}
