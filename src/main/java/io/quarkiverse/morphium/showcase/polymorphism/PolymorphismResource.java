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
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/polymorphism")
public class PolymorphismResource {

    @Inject
    Template polymorphism;

    @Inject
    @Location("tags/learn-polymorphism.html")
    Template learnPolymorphism;

    @Inject
    @Location("tags/demo-polymorphism.html")
    Template demoPolymorphism;

    @Inject
    Morphium morphium;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "polymorph=true, typeId, Type Hierarchies"),
            new DocLink("/docs/api-reference", "API Reference", "createQueryFor(), store(), dropCollection()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        seedIfEmpty();
        return polymorphism.data("active", "polymorphism")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnPolymorphism.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        seedIfEmpty();
        return demoData(null, null, null, null);
    }

    private TemplateInstance demoData(String success, String error,
            String lastOperation, String lastMongoCommand) {
        List<Vehicle> allVehicles = morphium.createQueryFor(Vehicle.class)
                .sort(Map.of(Vehicle.Fields.manufacturer, 1))
                .asList();
        List<Car> cars = allVehicles.stream().filter(v -> v instanceof Car).map(v -> (Car) v).collect(Collectors.toList());
        List<Truck> trucks = allVehicles.stream().filter(v -> v instanceof Truck).map(v -> (Truck) v).collect(Collectors.toList());
        return demoPolymorphism
                .data("vehicles", allVehicles)
                .data("cars", cars)
                .data("trucks", trucks)
                .data("totalCount", allVehicles.size())
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

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
            @FormParam("convertible") boolean convertible,
            @Context HttpHeaders headers) {
        Car car = new Car();
        car.setManufacturer(manufacturer);
        car.setModel(model);
        car.setYear(year);
        car.setPrice(price);
        car.setDoors(doors);
        car.setFuelType(fuelType);
        car.setConvertible(convertible);
        morphium.store(car);
        if (isHtmx(headers)) return Response.ok(demoData("Car added.", null,
                "morphium.store(car)", "db.vehicles.insertOne({..., class_name: \"Car\"})")).build();
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

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
            @FormParam("hasTowbar") boolean hasTowbar,
            @Context HttpHeaders headers) {
        Truck truck = new Truck();
        truck.setManufacturer(manufacturer);
        truck.setModel(model);
        truck.setYear(year);
        truck.setPrice(price);
        truck.setPayloadTons(payloadTons);
        truck.setAxles(axles);
        truck.setHasTowbar(hasTowbar);
        morphium.store(truck);
        if (isHtmx(headers)) return Response.ok(demoData("Truck added.", null,
                "morphium.store(truck)", "db.vehicles.insertOne({..., class_name: \"Truck\"})")).build();
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    @DELETE
    @Path("/vehicles/{id}")
    public Response deleteVehicle(@PathParam("id") String id, @Context HttpHeaders headers) {
        Vehicle vehicle = morphium.createQueryFor(Vehicle.class)
                .f(Vehicle.Fields.id).eq(new MorphiumId(id))
                .get();
        if (vehicle != null) morphium.delete(vehicle);
        if (isHtmx(headers)) return Response.ok(demoData("Vehicle deleted.", null,
                "morphium.delete(vehicle)", "db.vehicles.deleteOne({_id: ...})")).build();
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    @POST
    @Path("/seed")
    public Response seed(@Context HttpHeaders headers) {
        morphium.dropCollection(Vehicle.class);
        seedVehicles();
        if (isHtmx(headers)) return Response.ok(demoData("Sample data re-seeded.", null,
                "morphium.storeList(vehicles)", "db.vehicles.insertMany([...])")).build();
        return Response.seeOther(URI.create("/polymorphism")).build();
    }

    private void seedIfEmpty() {
        if (morphium.createQueryFor(Vehicle.class).countAll() == 0) seedVehicles();
    }

    private void seedVehicles() {
        Car car1 = new Car(); car1.setManufacturer("Porsche"); car1.setModel("911 Carrera"); car1.setYear(2024); car1.setPrice(115000.00); car1.setDoors(2); car1.setFuelType("Petrol"); car1.setConvertible(false);
        Car car2 = new Car(); car2.setManufacturer("Porsche"); car2.setModel("Taycan"); car2.setYear(2024); car2.setPrice(89000.00); car2.setDoors(4); car2.setFuelType("Electric"); car2.setConvertible(false);
        Car car3 = new Car(); car3.setManufacturer("Porsche"); car3.setModel("718 Boxster"); car3.setYear(2023); car3.setPrice(67000.00); car3.setDoors(2); car3.setFuelType("Petrol"); car3.setConvertible(true);
        Truck truck1 = new Truck(); truck1.setManufacturer("MAN"); truck1.setModel("TGX 18.510"); truck1.setYear(2024); truck1.setPrice(125000.00); truck1.setPayloadTons(18.5); truck1.setAxles(4); truck1.setHasTowbar(true);
        Truck truck2 = new Truck(); truck2.setManufacturer("Mercedes-Benz"); truck2.setModel("Actros L"); truck2.setYear(2023); truck2.setPrice(140000.00); truck2.setPayloadTons(25.0); truck2.setAxles(6); truck2.setHasTowbar(true);
        morphium.storeList(List.of(car1, car2, car3, truck1, truck2));
    }
}
