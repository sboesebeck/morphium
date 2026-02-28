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
package io.quarkiverse.morphium.showcase.geo;

import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

@Path("/geo")
public class GeoResource {

    @Inject
    Template geo;

    @Inject
    GeoService geoService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "GeoJSON, 2dsphere Indexes"),
            new DocLink("/docs/api-reference", "API Reference", "ensureIndicesFor(), store(), storeList()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        geoService.seedData();
        return geo.data("active", "geo")
                .data("stores", geoService.findAll())
                .data("count", geoService.count())
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/stores")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createStore(
            @FormParam("name") String name,
            @FormParam("address") String address,
            @FormParam("city") String city,
            @FormParam("country") String country,
            @FormParam("lng") double lng,
            @FormParam("lat") double lat,
            @FormParam("phone") String phone,
            @FormParam("services") String servicesStr) {
        List<String> services = servicesStr != null && !servicesStr.isBlank()
                ? Arrays.asList(servicesStr.split(",\\s*"))
                : List.of();
        geoService.create(name, address, city, country, lng, lat, phone, services);
        return Response.seeOther(URI.create("/geo")).build();
    }

    @POST
    @Path("/stores/nearby")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance findNearby(
            @FormParam("lng") double lng,
            @FormParam("lat") double lat,
            @FormParam("distance") double distance) {
        return geo.data("active", "geo")
                .data("stores", geoService.findNearby(lng, lat, distance))
                .data("count", geoService.count())
                .data("nearbySearch", true)
                .data("searchLng", lng)
                .data("searchLat", lat)
                .data("searchDistance", distance)
                .data("docLinks", DOC_LINKS);
    }

    @DELETE
    @Path("/stores/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteStore(@PathParam("id") String id) {
        geoService.delete(id);
        return Response.seeOther(URI.create("/geo")).build();
    }

    @POST
    @Path("/stores/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        geoService.deleteAll();
        geoService.seedData();
        return Response.seeOther(URI.create("/geo")).build();
    }
}