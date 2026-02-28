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
 * A car entity -- a concrete subclass of {@link Vehicle} demonstrating Morphium's polymorphic persistence.
 *
 * <p>Because both this class and its parent specify the same {@code collectionName = "vehicles"}
 * with {@code polymorph = true}, Car documents are stored in the shared "vehicles" collection.
 * Morphium writes an additional {@code class_name} field into each document so that when you
 * query for {@code Vehicle.class}, documents that were originally stored as {@code Car} are
 * automatically deserialized back into {@code Car} instances with all Car-specific fields intact.</p>
 *
 * <h3>Key points for subclass entities</h3>
 * <ul>
 *   <li>The subclass <strong>must</strong> repeat the {@code @Entity} annotation with the same
 *       {@code collectionName} and {@code polymorph = true}. Without it, Morphium would not
 *       recognize this class as a managed entity.</li>
 *   <li>Subclass-specific fields ({@code doors}, {@code fuelType}, {@code convertible}) are
 *       stored as additional fields in the same MongoDB document alongside the inherited fields.</li>
 *   <li>When querying for the base type ({@code Vehicle}), Morphium returns a mixed list of
 *       {@code Car}, {@code Truck}, and plain {@code Vehicle} instances -- true polymorphism.</li>
 * </ul>
 *
 * @see Vehicle
 * @see Truck
 */
// @Entity must be repeated on every subclass in a polymorphic hierarchy.
// The collectionName MUST match the parent's collection so all types share one MongoDB collection.
// polymorph = true is required on every class in the hierarchy for type discrimination to work.
@Entity(collectionName = "vehicles", polymorph = true)
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class Car extends Vehicle {

    /** Number of doors (e.g. 2 for a coupe, 4 for a sedan). */
    private int doors;

    /** Fuel / powertrain type (e.g. "Petrol", "Electric", "Hybrid"). */
    private String fuelType;

    /** Whether this car is a convertible / cabriolet. */
    private boolean convertible;
}