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

/**
 * JAX-RS resource for the geo/store-locator showcase, demonstrating Morphium's
 * <strong>CRUD operations</strong>, <strong>geospatial data handling</strong>,
 * and <strong>collection management</strong>.
 *
 * <p>This resource exposes endpoints for a store-locator application:</p>
 * <ul>
 *   <li>{@code GET /geo} -- Lists all stores (demonstrates {@code findAll()} query)</li>
 *   <li>{@code POST /geo/stores} -- Creates a new store (demonstrates {@code store()})</li>
 *   <li>{@code POST /geo/stores/nearby} -- Searches for nearby stores (geospatial placeholder)</li>
 *   <li>{@code DELETE /geo/stores/{id}} -- Deletes a store (demonstrates {@code delete()})</li>
 *   <li>{@code POST /geo/stores/seed} -- Resets and re-seeds demo data (demonstrates
 *       {@code dropCollection()} and {@code storeList()})</li>
 * </ul>
 *
 * <h3>Morphium Concepts Shown Indirectly</h3>
 * <p>All Morphium operations are delegated to {@link GeoService}. This resource demonstrates
 * the typical layered architecture pattern: REST resource -> Service -> Morphium ORM -> MongoDB.</p>
 *
 * @see GeoService for the Morphium CRUD and query operations
 * @see io.quarkiverse.morphium.showcase.geo.entity.Store the entity being managed
 */
@Path("/geo")
public class GeoResource {

    /** Qute template for the geo/store-locator HTML page. */
    @Inject
    Template geo;

    /** Service encapsulating all Morphium operations for store management. */
    @Inject
    GeoService geoService;

    /** Documentation links displayed on the geo page to guide learners. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "GeoJSON, 2dsphere Indexes"),
            new DocLink("/docs/api-reference", "API Reference", "ensureIndicesFor(), store(), storeList()")
    );

    /**
     * Renders the store locator page, listing all stores.
     *
     * <p>Seeds demo data on first access (idempotent) and then retrieves all stores
     * using {@code geoService.findAll()}, which calls {@code morphium.createQueryFor(Store.class).asList()}.</p>
     *
     * @return a Qute template instance populated with store data
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        // Ensure demo data exists. seedData() is idempotent (checks count > 0 first).
        geoService.seedData();
        return geo.data("active", "geo")
                .data("stores", geoService.findAll())
                .data("count", geoService.count())
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Creates a new store from form data and redirects back to the store list.
     *
     * <p>Delegates to {@link GeoService#create}, which calls {@code morphium.store(entity)}
     * to insert the new store document into MongoDB. The services field is parsed from a
     * comma-separated string into a {@code List<String>} before storage.</p>
     *
     * @param name        the store's display name
     * @param address     the street address
     * @param city        the city name
     * @param country     the country name
     * @param lng         longitude coordinate
     * @param lat         latitude coordinate
     * @param phone       the store's phone number
     * @param servicesStr comma-separated list of services (e.g., "Repair, Sales, Consultation")
     * @return a 303 See Other redirect to the store list page
     */
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
        // Parse comma-separated services string into a List for storage as a BSON array.
        List<String> services = servicesStr != null && !servicesStr.isBlank()
                ? Arrays.asList(servicesStr.split(",\\s*"))
                : List.of();
        geoService.create(name, address, city, country, lng, lat, phone, services);
        return Response.seeOther(URI.create("/geo")).build();
    }

    /**
     * Searches for stores near a given coordinate within a specified radius.
     *
     * <p>Currently a placeholder that returns all stores (see
     * {@link GeoService#findNearby} for details on why and how a proper geospatial
     * query would be implemented). The search parameters are passed back to the template
     * so the UI can display the search context.</p>
     *
     * @param lng      longitude of the search center point
     * @param lat      latitude of the search center point
     * @param distance maximum search radius in meters
     * @return a Qute template instance with search results and search parameters
     */
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

    /**
     * Deletes a single store by its ID.
     *
     * <p>Delegates to {@link GeoService#delete}, which uses the find-then-delete pattern:
     * first loads the entity via {@code query.f("id").eq(...).get()}, then calls
     * {@code morphium.delete(entity)}.</p>
     *
     * @param id the string representation of the store's MorphiumId/ObjectId
     * @return a 303 See Other redirect to the store list page
     */
    @DELETE
    @Path("/stores/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteStore(@PathParam("id") String id) {
        geoService.delete(id);
        return Response.seeOther(URI.create("/geo")).build();
    }

    /**
     * Resets the store collection and re-seeds it with demo data.
     *
     * <p>This endpoint demonstrates two destructive Morphium operations in sequence:</p>
     * <ol>
     *   <li>{@code geoService.deleteAll()} -- Calls {@code morphium.dropCollection(Store.class)},
     *       which completely removes the {@code stores} collection from MongoDB.</li>
     *   <li>{@code geoService.seedData()} -- Calls {@code morphium.storeList(stores)},
     *       which bulk-inserts the demo data. MongoDB automatically re-creates the
     *       collection on this first write.</li>
     * </ol>
     *
     * @return a 303 See Other redirect to the store list page
     */
    @POST
    @Path("/stores/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        // First drop the entire collection (removes all data AND indexes)...
        geoService.deleteAll();
        // ...then re-seed with fresh demo data via storeList().
        // Note: After dropCollection(), the indexes defined by @Index annotations on Store
        // will be re-created on the next ensureIndicesFor() call or on the next application
        // restart (when GeoService's @PostConstruct runs).
        geoService.seedData();
        return Response.seeOther(URI.create("/geo")).build();
    }
}