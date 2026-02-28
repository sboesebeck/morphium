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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

/**
 * A truck entity -- another concrete subclass of {@link Vehicle} in the polymorphic hierarchy.
 *
 * <p>Just like {@link Car}, this class shares the same {@code "vehicles"} collection. When Morphium
 * reads a document whose stored {@code class_name} field points to this class, it will instantiate
 * a {@code Truck} with all truck-specific fields ({@code payloadTons}, {@code axles},
 * {@code hasTowbar}) populated from the document.</p>
 *
 * <h3>MongoDB document structure (example)</h3>
 * <pre>{@code
 * {
 *   "_id": ObjectId("..."),
 *   "class_name": "io.quarkiverse.morphium.showcase.polymorphism.entity.Truck",
 *   "manufacturer": "MAN",
 *   "model": "TGX 18.510",
 *   "year": 2024,
 *   "price": 125000.0,
 *   "payload_tons": 18.5,
 *   "axles": 4,
 *   "has_towbar": true
 * }
 * }</pre>
 *
 * <p>Note how the inherited fields from {@link Vehicle} and the Truck-specific fields coexist
 * in a single flat document. The {@code class_name} field is the discriminator that Morphium
 * uses to pick the right Java class during deserialization.</p>
 *
 * @see Vehicle
 * @see Car
 */
// Same @Entity annotation as Vehicle and Car -- all three share the "vehicles" collection.
// polymorph = true ensures Morphium writes the class_name discriminator field.
@Entity(collectionName = "vehicles", polymorph = true)
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class Truck extends Vehicle {

    /** Maximum payload capacity in metric tons. */
    private double payloadTons;

    /** Number of axles on this truck (e.g. 4, 6). */
    private int axles;

    /** Whether this truck is equipped with a towbar for trailer coupling. */
    private boolean hasTowbar;
}