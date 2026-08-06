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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.driver.MorphiumId;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.core.MediaType;

/**
 * Minimal REST resource exercising {@link MorphiumId} on the JSON wire:
 * <ul>
 *   <li>{@code GET /morphium-id/entity} returns a {@link MorphiumIdEntity} — proves
 *       outbound serialization emits {@code "id":"<hex>"} instead of the struct.</li>
 *   <li>{@code GET /morphium-id/echo/{id}} echoes back a {@code MorphiumId} path
 *       param — proves inbound deserialization parses the hex string.</li>
 * </ul>
 * The extension installs the (de)serializer automatically; this resource writes
 * no custom JSON code.
 */
@Path("/morphium-id")
public class MorphiumIdResource {

    @GET
    @Path("/entity/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public MorphiumIdEntity entity(@PathParam("id") MorphiumId id) {
        MorphiumIdEntity e = new MorphiumIdEntity();
        e.setId(id);
        e.setName("widget");
        return e;
    }

    @POST
    @Path("/echo/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public String echo(@PathParam("id") MorphiumId id) {
        // Returning toString() proves the path param was parsed into a real
        // MorphiumId (not left as a raw string) and survives the round-trip.
        return id.toString();
    }

    @POST
    @Path("/entity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String acceptEntity(MorphiumIdEntity entity) {
        // Exercises MorphiumIdJacksonModule's deserializer via the JSON request-body path
        // (distinct from the @PathParam String-constructor path both other endpoints use).
        return entity.getId().toString();
    }
}
