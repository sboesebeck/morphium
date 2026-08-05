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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import de.caluga.morphium.driver.MorphiumId;

import io.quarkus.jackson.ObjectMapperCustomizer;

import jakarta.inject.Singleton;

/**
 * Registers a Jackson {@link com.fasterxml.jackson.databind.Module Module} that
 * (de)serializes {@link MorphiumId} as its canonical 24-character hex string.
 *
 * <p><b>Why this exists.</b> Without a custom serializer Jackson walks the
 * getters of {@code MorphiumId} ({@code getPid()}, {@code getCounter()},
 * {@code getMachineId()}, {@code getBytes()}, {@code getTime()}) and emits the
 * internal struct:
 * <pre>{@code {"pid":..,"counter":..,"machineId":..,"bytes":"..","time":..}}</pre>
 * Frontend grids that key rows by id call {@code String(row.id)} on that object
 * and get the literal {@code "[object Object]"} — every row collapses to the
 * same key, row identity is lost, and the grid re-renders every cell on each
 * change-detection tick (flicker, lost focus, runaway memory, renderer crash).
 * The hex string is the only usable wire form of an id.
 *
 * <p>The deserializer mirrors the serializer so REST endpoints accepting a
 * {@code MorphiumId} as a path/query/body parameter parse the hex string back
 * into a real {@code MorphiumId}.
 *
 * <p>This bean is registered automatically by the extension's build-time
 * processor when {@code quarkus-jackson} is on the classpath; consumers do not
 * need to declare it. Jackson is an <em>optional</em> dependency of the
 * extension, so this class is only loaded when a Jackson-based JSON layer
 * (e.g. {@code quarkus-rest-jackson}) is actually present.
 */
@Singleton
public class MorphiumIdJacksonModule implements ObjectMapperCustomizer {

    @Override
    public void customize(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule("MorphiumIdModule");

        module.addSerializer(MorphiumId.class, new StdSerializer<>(MorphiumId.class) {
            @Override
            public void serialize(MorphiumId value, JsonGenerator gen, SerializerProvider provider)
                    throws IOException {
                gen.writeString(value.toString());
            }
        });

        module.addDeserializer(MorphiumId.class, new StdDeserializer<>(MorphiumId.class) {
            @Override
            public MorphiumId deserialize(JsonParser parser, DeserializationContext ctx)
                    throws IOException {
                String hex = parser.getValueAsString();
                return hex == null || hex.isBlank() ? null : new MorphiumId(hex);
            }
        });

        mapper.registerModule(module);
    }
}
