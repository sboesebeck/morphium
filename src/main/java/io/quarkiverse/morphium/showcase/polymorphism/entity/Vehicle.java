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
package io.quarkiverse.morphium.showcase.polymorphism.entity;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

/**
 * Base entity for the polymorphism showcase demonstrating Morphium's inheritance mapping.
 *
 * <p>This class serves as the root of a type hierarchy stored in a single MongoDB collection.
 * Morphium supports polymorphic persistence: subclasses like {@link Car} and {@link Truck}
 * are stored alongside their parent in the <em>same</em> collection ({@code "vehicles"}).
 * When documents are read back, Morphium automatically instantiates the correct Java subtype.</p>
 *
 * <h3>How polymorphism works in Morphium</h3>
 * <ol>
 *   <li>The {@code @Entity(polymorph = true)} annotation tells Morphium to store a
 *       {@code class_name} (or {@code _. type_id}) field in each document.</li>
 *   <li>On read, Morphium inspects that field and instantiates the correct Java class,
 *       even when you query for the base type {@code Vehicle}.</li>
 *   <li>All classes in the hierarchy <strong>must</strong> declare the same
 *       {@code collectionName} so they share one MongoDB collection.</li>
 * </ol>
 *
 * <h3>Annotations explained</h3>
 * <ul>
 *   <li>{@code @Entity(collectionName = "vehicles", polymorph = true)} --
 *       Maps this class to the "vehicles" collection and enables polymorphic type storage.</li>
 *   <li>{@code @Id} -- Marks the primary key field. Morphium maps it to MongoDB's {@code _id}.
 *       Using {@link MorphiumId} gives you a BSON ObjectId.</li>
 *   <li>{@code @FieldNameConstants} (Lombok) -- Generates a static inner class {@code Fields}
 *       with string constants for each field name, useful for type-safe queries and sorting.</li>
 * </ul>
 *
 * @see Car
 * @see Truck
 */
// @Entity: Declares this class as a Morphium-managed entity.
//   - collectionName: explicitly sets the MongoDB collection name (default would be the class name in snake_case).
//   - polymorph: when true, Morphium stores the fully-qualified class name in each document so it
//     can reconstruct the correct Java subtype on read.
@Entity(collectionName = "vehicles", polymorph = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class Vehicle {

    // @Id: Marks this field as the document's primary key, mapped to MongoDB's "_id" field.
    // MorphiumId is Morphium's own ObjectId type, equivalent to BSON ObjectId.
    // If left null when storing, Morphium (or MongoDB) will auto-generate an ObjectId.
    @Id
    private MorphiumId id;

    /** Manufacturer / brand name of the vehicle (e.g. "Porsche", "MAN"). */
    private String manufacturer;

    /** Model designation (e.g. "911 Carrera", "TGX 18.510"). */
    private String model;

    /** Model year of the vehicle. */
    private int year;

    /** Base price in the default currency. */
    private double price;

    /**
     * Returns the simple class name of the actual runtime type.
     *
     * <p>This is a convenience method for the UI layer -- because Morphium deserializes
     * documents into their correct subtype (Car, Truck, etc.), calling this on any
     * {@code Vehicle} reference will return the specific type name, demonstrating
     * that polymorphic deserialization preserves the original Java type.</p>
     *
     * @return the simple class name, e.g. "Car" or "Truck"
     */
    public String getTypeName() {
        return getClass().getSimpleName();
    }
}