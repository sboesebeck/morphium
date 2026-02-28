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

@ApplicationScoped
public class GeoService {

    @Inject
    Morphium morphium;

    @PostConstruct
    void init() {
        morphium.ensureIndicesFor(Store.class);
    }

    public List<Store> findAll() {
        return morphium.createQueryFor(Store.class).asList();
    }

    public Store findById(String id) {
        return morphium.createQueryFor(Store.class)
                .f(Store.Fields.id).eq(new MorphiumId(id))
                .get();
    }

    public Store create(String name, String address, String city, String country,
                        double lng, double lat, String phone, List<String> services) {
        Store store = Store.builder()
                .name(name)
                .address(address)
                .city(city)
                .country(country)
                .location(new double[]{lng, lat})
                .phone(phone)
                .services(services)
                .build();
        morphium.store(store);
        return store;
    }

    /**
     * Find stores near a given coordinate.
     * TODO: For proper geo-spatial queries, use a MongoDB $nearSphere command via the driver.
     * Morphium's Query API does not have built-in geo-spatial operators, so a full
     * implementation would require morphium.getDriver().runCommand() with a $nearSphere filter.
     * For now, this returns all stores as a placeholder.
     */
    public List<Store> findNearby(double lng, double lat, double maxDistanceMeters) {
        // TODO: Implement proper geo-spatial query using driver-level command:
        //   { find: "stores", filter: { location: { $nearSphere: {
        //       $geometry: { type: "Point", coordinates: [lng, lat] },
        //       $maxDistance: maxDistanceMeters } } } }
        return morphium.createQueryFor(Store.class).asList();
    }

    public void delete(String id) {
        Store store = findById(id);
        if (store != null) {
            morphium.delete(store);
        }
    }

    public void deleteAll() {
        morphium.dropCollection(Store.class);
    }

    public long count() {
        return morphium.createQueryFor(Store.class).countAll();
    }

    public void seedData() {
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
        morphium.storeList(stores);
    }
}