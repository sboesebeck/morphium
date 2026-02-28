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
package io.quarkiverse.morphium.showcase.polymorphism;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkiverse.morphium.showcase.polymorphism.entity.Car;
import io.quarkiverse.morphium.showcase.polymorphism.entity.Truck;
import io.quarkiverse.morphium.showcase.polymorphism.entity.Vehicle;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JAX-RS resource demonstrating Morphium's <strong>polymorphic persistence</strong> features.
 *
 * <p>This resource showcases the core Morphium APIs for CRUD operations on a type hierarchy
 * ({@link Vehicle} &rarr; {@link Car}, {@link Truck}) stored in a single MongoDB collection.
 * It demonstrates:</p>
 * <ul>
 *   <li><strong>Polymorphic querying:</strong> Querying for the base type {@code Vehicle} returns
 *       a mixed list of {@code Car} and {@code Truck} instances, each correctly deserialized.</li>
 *   <li><strong>Type-safe field references:</strong> Using Lombok-generated {@code Fields} constants
 *       for sort keys instead of raw strings.</li>
 *   <li><strong>Store / delete / drop operations:</strong> The standard Morphium write API.</li>
 *   <li><strong>Batch storage:</strong> Using {@code storeList()} to persist multiple entities
 *       in a single call.</li>
 * </ul>
 */
@Path("/polymorphism")
public class PolymorphismResource {

    @Inject
    Template polymorphism;

    /** The central Morphium instance -- the main entry point for all database operations. */
    @Inject
    Morphium morphium;

    /** Documentation links displayed on the showcase page for cross-referencing. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "polymorph=true, typeId, Type Hierarchies"),
            new DocLink("/docs/api-reference", "API Reference", "createQueryFor(), store(), dropCollection()")
    );

    /**
     * Renders the polymorphism showcase page.
     *
     * <p>Demonstrates a polymorphic query: querying for {@code Vehicle.class} returns all
     * documents from the "vehicles" collection, automatically deserialized into their correct
     * subtypes ({@link Car} or {@link Truck}). The result list can then be filtered by type
     * using standard Java instanceof checks.</p>
     *
     * @return a Qute template instance with vehicles, cars, trucks, and documentation links
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        seedIfEmpty();

        // createQueryFor(Vehicle.class): Creates a Morphium Query targeting the "vehicles" collection.
        // Because Vehicle is the base class with polymorph=true, this query returns ALL documents
        // in the collection -- including Car and Truck subtypes.
        //
        // .sort(Map.of(field, 1)): Sorts ascending by the given field.
        // Vehicle.Fields.manufacturer is a Lombok-generated constant ("manufacturer"),
        // providing compile-time safety instead of a raw string.
        //
        // .asList(): Executes the query and returns all matching documents as a Java List.
        // Each document is deserialized into its correct Java type based on the stored class_name.
        List<Vehicle> allVehicles = morphium.createQueryFor(Vehicle.class)
                .sort(Map.of(Vehicle.Fields.manufacturer, 1))
                .asList();

        // After the polymorphic query, we can use Java's type system to separate the results.
        // This demonstrates that Morphium correctly preserves the original Java type.
        List<Car> cars = allVehicles.stream()
                .filter(v -> v instanceof Car)
                .map(v -> (Car) v)
                .collect(Collectors.toList());

        List<Truck> trucks = allVehicles.stream()
                .filter(v -> v instanceof Truck)
                .map(v -> (Truck) v)
                .collect(Collectors.toList());

        return polymorphism.data("active", "polymorphism")
                .data("vehicles", allVehicles)
                .data("cars", cars)
                .data("trucks", trucks)
                .data("totalCount", allVehicles.size())
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Creates a new {@link Car} entity from form data and stores it in MongoDB.
     *
     * <p>Demonstrates {@code morphium.store()}: Morphium inspects the object's class,
     * determines the target collection from the {@code @Entity} annotation, serializes
     * all fields (including inherited ones from {@link Vehicle}), and inserts the document.
     * Because {@code polymorph = true}, the fully-qualified class name is also stored.</p>
     *
     * @param manufacturer the car manufacturer
     * @param model        the car model
     * @param year         the model year
     * @param price        the base price
     * @param doors        number of doors
     * @param fuelType     fuel / powertrain type
     * @param convertible  whether the car is a convertible
     * @return a redirect to the polymorphism page (POST-Redirect-GET pattern)
     */
    @POST
    @Path("/cars")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createCar(
            @FormParam("manufacturer") String manufacturer,
            @FormParam("model") String model,
            @FormParam("year") int year,
            @FormParam("price") double price,
            @FormParam("doors") int doors,
            @FormParam("fuelType") String fuelType,
            @FormParam("convertible") boolean convertible) {
        Car car = new Car();
        car.setManufacturer(manufacturer);
        car.setModel(model);
        car.setYear(year);
        car.setPrice(price);
        car.setDoors(doors);
        car.setFuelType(fuelType);
        car.setConvertible(convertible);

        // morphium.store(): Persists the entity to MongoDB. Because the id field is null,
        // Morphium performs an INSERT and MongoDB auto-generates an ObjectId.
        // If the id were already set, Morphium would perform an UPSERT instead.
        morphium.store(car);
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    /**
     * Creates a new {@link Truck} entity from form data and stores it in MongoDB.
     *
     * @param manufacturer the truck manufacturer
     * @param model        the truck model
     * @param year         the model year
     * @param price        the base price
     * @param payloadTons  maximum payload in metric tons
     * @param axles        number of axles
     * @param hasTowbar    whether the truck has a towbar
     * @return a redirect to the polymorphism page
     */
    @POST
    @Path("/trucks")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createTruck(
            @FormParam("manufacturer") String manufacturer,
            @FormParam("model") String model,
            @FormParam("year") int year,
            @FormParam("price") double price,
            @FormParam("payloadTons") double payloadTons,
            @FormParam("axles") int axles,
            @FormParam("hasTowbar") boolean hasTowbar) {
        Truck truck = new Truck();
        truck.setManufacturer(manufacturer);
        truck.setModel(model);
        truck.setYear(year);
        truck.setPrice(price);
        truck.setPayloadTons(payloadTons);
        truck.setAxles(axles);
        truck.setHasTowbar(hasTowbar);

        // Same store() call -- Morphium determines the collection from @Entity on Truck.
        morphium.store(truck);
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    /**
     * Deletes a vehicle by its MongoDB ObjectId.
     *
     * <p>Demonstrates Morphium's query-by-id pattern: create a query, filter by the {@code id}
     * field using {@code .f().eq()}, fetch the single result with {@code .get()}, then
     * call {@code morphium.delete()} to remove it.</p>
     *
     * @param id the string representation of the MorphiumId (MongoDB ObjectId)
     * @return a redirect to the polymorphism page
     */
    @DELETE
    @Path("/vehicles/{id}")
    public Response deleteVehicle(@PathParam("id") String id) {
        // createQueryFor(Vehicle.class): Even though we may be deleting a Car or Truck,
        // we query by the base type. The polymorphic mapping ensures the correct subtype
        // is returned.
        //
        // .f(fieldName): Starts a field filter (short for "field"). The Fields constant
        // provides compile-time safety for the field name.
        //
        // .eq(value): Adds an equality constraint. Here we match by _id.
        //
        // .get(): Returns a single result (or null if not found).
        Vehicle vehicle = morphium.createQueryFor(Vehicle.class)
                .f(Vehicle.Fields.id).eq(new MorphiumId(id))
                .get();
        if (vehicle != null) {
            // morphium.delete(): Removes the document from MongoDB.
            // Morphium identifies the document by its @Id field.
            morphium.delete(vehicle);
        }
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    /**
     * Resets the vehicles collection and re-seeds it with sample data.
     *
     * <p>Demonstrates {@code morphium.dropCollection()}: drops the entire MongoDB collection
     * associated with the given entity class. This is a destructive operation that removes
     * all documents and indexes.</p>
     *
     * @return a redirect to the polymorphism page
     */
    @POST
    @Path("/seed")
    public Response seed() {
        // dropCollection(): Drops the entire "vehicles" collection from MongoDB.
        // All documents (Cars, Trucks, plain Vehicles) are removed since they share one collection.
        morphium.dropCollection(Vehicle.class);
        seedVehicles();
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    /**
     * Seeds the collection with sample data only if it is currently empty.
     * Uses {@code countAll()} to check the document count without loading any data.
     */
    private void seedIfEmpty() {
        // countAll(): Efficiently counts all documents matching the query (here: all vehicles).
        // This is a server-side count -- no documents are transferred to the application.
        if (morphium.createQueryFor(Vehicle.class).countAll() == 0) {
            seedVehicles();
        }
    }

    /**
     * Populates the vehicles collection with a mix of Car and Truck entities.
     *
     * <p>Demonstrates {@code morphium.storeList()}: persists multiple entities in a single call.
     * Morphium will batch-insert them, which is more efficient than individual {@code store()}
     * calls. Note that the list contains a mix of subtypes (Car and Truck) -- Morphium handles
     * each one according to its actual runtime type and stores the correct {@code class_name}
     * discriminator for each document.</p>
     */
    private void seedVehicles() {
        Car car1 = new Car();
        car1.setManufacturer("Porsche");
        car1.setModel("911 Carrera");
        car1.setYear(2024);
        car1.setPrice(115000.00);
        car1.setDoors(2);
        car1.setFuelType("Petrol");
        car1.setConvertible(false);

        Car car2 = new Car();
        car2.setManufacturer("Porsche");
        car2.setModel("Taycan");
        car2.setYear(2024);
        car2.setPrice(89000.00);
        car2.setDoors(4);
        car2.setFuelType("Electric");
        car2.setConvertible(false);

        Car car3 = new Car();
        car3.setManufacturer("Porsche");
        car3.setModel("718 Boxster");
        car3.setYear(2023);
        car3.setPrice(67000.00);
        car3.setDoors(2);
        car3.setFuelType("Petrol");
        car3.setConvertible(true);

        Truck truck1 = new Truck();
        truck1.setManufacturer("MAN");
        truck1.setModel("TGX 18.510");
        truck1.setYear(2024);
        truck1.setPrice(125000.00);
        truck1.setPayloadTons(18.5);
        truck1.setAxles(4);
        truck1.setHasTowbar(true);

        Truck truck2 = new Truck();
        truck2.setManufacturer("Mercedes-Benz");
        truck2.setModel("Actros L");
        truck2.setYear(2023);
        truck2.setPrice(140000.00);
        truck2.setPayloadTons(25.0);
        truck2.setAxles(6);
        truck2.setHasTowbar(true);

        // storeList(): Batch-inserts all entities in one call. Morphium serializes each entity
        // according to its actual type (Car or Truck), including the class_name discriminator.
        // This is significantly more efficient than calling store() in a loop.
        morphium.storeList(List.of(car1, car2, car3, truck1, truck2));
    }
}