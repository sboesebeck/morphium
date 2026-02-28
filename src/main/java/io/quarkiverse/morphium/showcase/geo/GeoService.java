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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.geo.entity.Store;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

/**
 * Service demonstrating Morphium's <strong>CRUD operations</strong>, <strong>index management</strong>,
 * <strong>collection lifecycle</strong>, and <strong>geospatial data handling</strong>.
 *
 * <p>This service is a comprehensive showcase of the most common Morphium ORM operations
 * that developers use in real applications. It covers:</p>
 *
 * <h3>Morphium Features Demonstrated</h3>
 * <ul>
 *   <li><strong>{@code morphium.ensureIndicesFor(Class)}</strong> -- Scans the entity class for
 *       {@code @Index} annotations and creates the corresponding MongoDB indexes if they
 *       do not already exist. Called in {@code @PostConstruct} to ensure indexes are ready
 *       before any queries run.</li>
 *   <li><strong>{@code morphium.createQueryFor(Class)}</strong> -- Creates a type-safe query
 *       builder for the given entity class.</li>
 *   <li><strong>{@code query.f(field).eq(value)}</strong> -- The fluent filter API.
 *       {@code f()} selects a field, and {@code eq()} adds an equality condition.
 *       Other operators include {@code ne()}, {@code gt()}, {@code lt()}, {@code in()},
 *       {@code exists()}, etc.</li>
 *   <li><strong>{@code query.get()}</strong> -- Executes the query and returns a single result
 *       (or {@code null} if not found). Use {@code asList()} for multiple results.</li>
 *   <li><strong>{@code morphium.store(entity)}</strong> -- Inserts a new entity or updates an
 *       existing one (upsert based on the {@code @Id} field).</li>
 *   <li><strong>{@code morphium.delete(entity)}</strong> -- Deletes a single entity by its
 *       {@code @Id} field.</li>
 *   <li><strong>{@code morphium.dropCollection(Class)}</strong> -- Drops the entire MongoDB
 *       collection, removing all documents and indexes. Use with caution!</li>
 *   <li><strong>{@code morphium.storeList(list)}</strong> -- Bulk-inserts multiple entities
 *       in a single operation for better performance.</li>
 *   <li><strong>{@code query.countAll()}</strong> -- Counts all documents matching the query
 *       (or all documents if no filter is applied).</li>
 * </ul>
 *
 * @see Store the entity this service manages
 * @see GeoResource the REST resource that exposes these operations
 */
@ApplicationScoped
public class GeoService {

    /**
     * The Morphium ORM instance, injected by the Quarkus CDI container.
     * All database operations go through this single, thread-safe instance.
     */
    @Inject
    Morphium morphium;

    /**
     * Ensures MongoDB indexes exist for the {@link Store} entity on application startup.
     *
     * <p>{@code morphium.ensureIndicesFor(Store.class)} scans the entity class for all
     * {@code @Index} annotations (both field-level and class-level) and sends
     * {@code createIndex} commands to MongoDB for any that do not already exist. This is
     * an idempotent operation -- calling it multiple times is safe.</p>
     *
     * <p>This is called in {@code @PostConstruct} so that indexes are guaranteed to be
     * ready before any queries are executed. An alternative approach is to set
     * {@code autoIndexAndCappedCreationOnWrite = true} in Morphium's configuration, which
     * creates indexes lazily on first write.</p>
     */
    @PostConstruct
    void init() {
        // ensureIndicesFor() -- Reads @Index annotations from Store.class and creates
        // the corresponding MongoDB indexes. For Store, this creates a single-field index
        // on "name". This is a no-op if the indexes already exist.
        morphium.ensureIndicesFor(Store.class);
    }

    /**
     * Retrieves all stores from the database.
     *
     * <p>Demonstrates the simplest possible Morphium query: create a query with no
     * filters and call {@code asList()} to get all documents in the collection.</p>
     *
     * @return a list of all {@link Store} entities in the {@code stores} collection
     */
    public List<Store> findAll() {
        // createQueryFor() with no filters + asList() returns ALL documents.
        // In production, you would add pagination via limit() and skip().
        return morphium.createQueryFor(Store.class).asList();
    }

    /**
     * Finds a single store by its MongoDB ObjectId.
     *
     * <p>Demonstrates Morphium's fluent query API with field filtering:</p>
     * <ul>
     *   <li>{@code f(Store.Fields.id)} -- Selects the field to filter on. Using Lombok's
     *       {@code @FieldNameConstants} provides a type-safe field reference.</li>
     *   <li>{@code eq(new MorphiumId(id))} -- Adds an equality filter. The string ID is
     *       converted to a {@link MorphiumId} to match the field's type.</li>
     *   <li>{@code get()} -- Executes the query and returns a single document (or {@code null}).
     *       This is more efficient than {@code asList()} when you only need one result,
     *       as it adds an implicit {@code limit(1)}.</li>
     * </ul>
     *
     * @param id the string representation of the store's MorphiumId/ObjectId
     * @return the matching {@link Store}, or {@code null} if not found
     */
    public Store findById(String id) {
        // f() -- Starts a field filter. The string passed is the Java field name.
        // eq() -- Adds an equality condition ($eq in MongoDB).
        // get() -- Executes the query and returns a single result (null if not found).
        return morphium.createQueryFor(Store.class)
                .f(Store.Fields.id).eq(new MorphiumId(id))
                .get();
    }

    /**
     * Creates and persists a new store entity.
     *
     * <p>Demonstrates {@code morphium.store(entity)} for single-document inserts.
     * Since the entity's {@code id} field is {@code null} (not set by the builder),
     * Morphium treats this as an insert. After the call, the entity's {@code id}
     * field is populated with the MongoDB-generated ObjectId.</p>
     *
     * @param name     the store's display name
     * @param address  the street address
     * @param city     the city name
     * @param country  the country name
     * @param lng      longitude coordinate (GeoJSON order: longitude first)
     * @param lat      latitude coordinate
     * @param phone    the store's phone number
     * @param services list of services offered (e.g., "Repair", "Sales")
     * @return the newly created store with its auto-generated ID populated
     */
    public Store create(String name, String address, String city, String country,
                        double lng, double lat, String phone, List<String> services) {
        Store store = Store.builder()
                .name(name)
                .address(address)
                .city(city)
                .country(country)
                // GeoJSON convention: [longitude, latitude] -- NOT [lat, lng]!
                .location(new double[]{lng, lat})
                .phone(phone)
                .services(services)
                .build();

        // store() -- Persists the entity. Since id is null, this is an INSERT.
        // After this call, store.getId() will return the generated MorphiumId.
        morphium.store(store);
        return store;
    }

    /**
     * Find stores near a given coordinate.
     *
     * <p><strong>Note:</strong> This is currently a placeholder that returns all stores.
     * Morphium's Query API does not have built-in geospatial query operators
     * ({@code $nearSphere}, {@code $geoWithin}, etc.). To implement proper geospatial
     * queries, you would need to use Morphium's driver-level command execution:</p>
     *
     * <pre>{@code
     * // Example of how a proper geo query would look using the driver:
     * morphium.getDriver().runCommand("stores", Map.of(
     *     "find", "stores",
     *     "filter", Map.of("location", Map.of(
     *         "$nearSphere", Map.of(
     *             "$geometry", Map.of("type", "Point", "coordinates", List.of(lng, lat)),
     *             "$maxDistance", maxDistanceMeters
     *         )
     *     ))
     * ));
     * }</pre>
     *
     * <p>This requires a 2dsphere index on the {@code location} field, which can be
     * created via the MongoDB shell: {@code db.stores.createIndex({location: "2dsphere"})}</p>
     *
     * @param lng                longitude of the search center point
     * @param lat                latitude of the search center point
     * @param maxDistanceMeters  maximum search radius in meters
     * @return all stores (placeholder -- see TODO for proper implementation)
     */
    public List<Store> findNearby(double lng, double lat, double maxDistanceMeters) {
        // TODO: Implement proper geo-spatial query using driver-level command:
        //   { find: "stores", filter: { location: { $nearSphere: {
        //       $geometry: { type: "Point", coordinates: [lng, lat] },
        //       $maxDistance: maxDistanceMeters } } } }
        return morphium.createQueryFor(Store.class).asList();
    }

    /**
     * Deletes a single store by its ID.
     *
     * <p>Demonstrates Morphium's {@code delete(entity)} method, which removes a document
     * from MongoDB based on the entity's {@code @Id} field. The entity must have a
     * non-null ID for deletion to work.</p>
     *
     * <p>The pattern here is "find-then-delete": first load the entity to verify it exists,
     * then delete it. An alternative is {@code morphium.createQueryFor(Store.class).f("id").eq(...).delete()},
     * which deletes directly without loading the entity first.</p>
     *
     * @param id the string representation of the store's MorphiumId to delete
     */
    public void delete(String id) {
        Store store = findById(id);
        if (store != null) {
            // delete(entity) -- Deletes the document matching the entity's @Id from MongoDB.
            // This sends a deleteOne command with { _id: <entity's id> } as the filter.
            morphium.delete(store);
        }
    }

    /**
     * Drops the entire {@code stores} collection, removing all documents and indexes.
     *
     * <p>{@code morphium.dropCollection(Class)} sends a MongoDB {@code drop} command for
     * the collection associated with the given entity class. This is a <strong>destructive
     * operation</strong> -- all data and indexes are permanently removed. After dropping,
     * the collection will be automatically re-created (as a regular collection) on the
     * next write operation.</p>
     *
     * <p>This is used here to support the "reset and re-seed" functionality in the demo.
     * In production code, you would rarely use this -- prefer {@code deleteMany()} via
     * a query for selective deletion.</p>
     */
    public void deleteAll() {
        // dropCollection() -- Completely removes the MongoDB collection and all its indexes.
        // The collection (and indexes) will be re-created on the next store() or storeList() call.
        morphium.dropCollection(Store.class);
    }

    /**
     * Returns the total number of stores in the collection.
     *
     * @return the document count for the {@code stores} collection
     */
    public long count() {
        // countAll() -- Executes a count command with no filter, returning the total
        // number of documents in the collection.
        return morphium.createQueryFor(Store.class).countAll();
    }

    /**
     * Seeds the database with sample store locations across German cities.
     *
     * <p>Demonstrates two Morphium features:</p>
     * <ul>
     *   <li>{@code countAll()} -- Used as an idempotency check to avoid re-seeding.</li>
     *   <li>{@code morphium.storeList(list)} -- Bulk-inserts multiple entities in a single
     *       operation. This is significantly more efficient than calling {@code store()}
     *       individually for each entity because Morphium sends a single bulk write
     *       command to MongoDB.</li>
     * </ul>
     *
     * <p>Note the coordinate format: {@code [longitude, latitude]}. For example, Munich
     * is at longitude 11.576 (east) and latitude 48.137 (north).</p>
     */
    public void seedData() {
        // Idempotency guard: only seed if the collection is empty.
        if (count() > 0) return;

        List<Store> stores = List.of(
                Store.builder()
                        .name("Munich Store")
                        .address("Marienplatz 1")
                        .city("Munich")
                        .country("Germany")
                        .location(new double[]{11.5760, 48.1374})
                        .phone("+49 89 1234567")
                        .services(List.of("Repair", "Sales", "Consultation"))
                        .build(),
                Store.builder()
                        .name("Berlin Store")
                        .address("Unter den Linden 77")
                        .city("Berlin")
                        .country("Germany")
                        .location(new double[]{13.3889, 52.5170})
                        .phone("+49 30 2345678")
                        .services(List.of("Sales", "Consultation"))
                        .build(),
                Store.builder()
                        .name("Hamburg Store")
                        .address("Jungfernstieg 10")
                        .city("Hamburg")
                        .country("Germany")
                        .location(new double[]{9.9937, 53.5511})
                        .phone("+49 40 3456789")
                        .services(List.of("Repair", "Sales"))
                        .build(),
                Store.builder()
                        .name("Frankfurt Store")
                        .address("Zeil 106")
                        .city("Frankfurt")
                        .country("Germany")
                        .location(new double[]{8.6821, 50.1109})
                        .phone("+49 69 4567890")
                        .services(List.of("Sales", "Consultation", "Pickup"))
                        .build(),
                Store.builder()
                        .name("Stuttgart Store")
                        .address("Koenigstrasse 28")
                        .city("Stuttgart")
                        .country("Germany")
                        .location(new double[]{9.1829, 48.7758})
                        .phone("+49 711 5678901")
                        .services(List.of("Repair", "Sales", "Consultation", "Pickup"))
                        .build(),
                Store.builder()
                        .name("Cologne Store")
                        .address("Hohe Strasse 52")
                        .city("Cologne")
                        .country("Germany")
                        .location(new double[]{6.9603, 50.9375})
                        .phone("+49 221 6789012")
                        .services(List.of("Sales", "Pickup"))
                        .build()
        );

        // storeList() -- Bulk-inserts all 6 stores in a single write operation.
        // Much more efficient than 6 individual store() calls. After this call,
        // each Store object's id field will be populated with the generated MorphiumId.
        morphium.storeList(stores);
    }
}