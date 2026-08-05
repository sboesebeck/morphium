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

import com.fasterxml.jackson.databind.ObjectMapper;

import de.caluga.morphium.driver.MorphiumId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link MorphiumIdJacksonModule} serializes {@link MorphiumId}
 * as a flat hex string (not the internal bean struct) and parses it back.
 */
@DisplayName("MorphiumId Jackson (de)serialization")
class MorphiumIdJacksonModuleTest {

    private ObjectMapper mapperWithModule() {
        ObjectMapper mapper = new ObjectMapper();
        new MorphiumIdJacksonModule().customize(mapper);
        return mapper;
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

        String json = mapperWithModule().writeValueAsString(doc);

        assertThat(json).contains("\"id\":\"" + id + "\"");
        // None of the internal getters must leak into the JSON.
        assertThat(json).doesNotContain("pid");
        assertThat(json).doesNotContain("counter");
        assertThat(json).doesNotContain("machineId");
        assertThat(json).doesNotContain("bytes");
    }

    @Test
    @DisplayName("a bare MorphiumId serializes to a JSON string literal")
    void bareIdSerializesToStringLiteral() throws Exception {
        MorphiumId id = new MorphiumId();
        String json = mapperWithModule().writeValueAsString(id);
        assertThat(json).isEqualTo("\"" + id + "\"");
    }

    @Test
    @DisplayName("\"<hex>\" deserializes back into an equal MorphiumId")
    void deserializesFromHexString() throws Exception {
        MorphiumId id = new MorphiumId();
        ObjectMapper mapper = mapperWithModule();

        String json = "{\"id\":\"" + id + "\",\"name\":\"widget\"}";
        Doc parsed = mapper.readValue(json, Doc.class);

        assertThat(parsed.id).isEqualTo(id);
        assertThat(parsed.name).isEqualTo("widget");
    }

    @Test
    @DisplayName("round-trips entity -> JSON -> entity preserving id identity")
    void roundTrips() throws Exception {
        MorphiumId id = new MorphiumId();
        Doc doc = new Doc();
        doc.id = id;
        doc.name = "round";

        ObjectMapper mapper = mapperWithModule();
        Doc back = mapper.readValue(mapper.writeValueAsString(doc), Doc.class);

        assertThat(back.id).isEqualTo(id);
    }

    @Test
    @DisplayName("null and blank id strings deserialize to null")
    void nullAndBlankDeserializeToNull() throws Exception {
        ObjectMapper mapper = mapperWithModule();

        assertThat(mapper.readValue("{\"id\":null}", Doc.class).id).isNull();
        assertThat(mapper.readValue("{\"id\":\"\"}", Doc.class).id).isNull();
    }
}
