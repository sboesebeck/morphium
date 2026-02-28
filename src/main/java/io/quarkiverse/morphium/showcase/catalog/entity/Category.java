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
package io.quarkiverse.morphium.showcase.catalog.entity;

import de.caluga.morphium.annotations.Embedded;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

/**
 * An embedded document representing a product category.
 *
 * <p>This class demonstrates Morphium's {@code @Embedded} annotation, which is used for objects that
 * should be stored as nested sub-documents inside a parent entity's MongoDB document rather than in
 * their own separate collection.</p>
 *
 * <h3>@Embedded vs @Entity -- When to use which?</h3>
 * <ul>
 *   <li><strong>{@code @Embedded}</strong>: The object is stored inline within its parent document.
 *       It does NOT get its own collection and does NOT need an {@code @Id} field. Use this for
 *       value-like objects that are always accessed together with their parent (e.g., an address
 *       within an order, or a category within a product).</li>
 *   <li><strong>{@code @Entity}</strong>: The object gets its own MongoDB collection and requires
 *       an {@code @Id} field. Use this for independent domain objects that may be referenced from
 *       multiple places or queried independently.</li>
 * </ul>
 *
 * <p>When a {@code Category} is stored inside a {@link Product}, the resulting MongoDB document looks
 * like:</p>
 * <pre>{@code
 * {
 *   "_id": ObjectId("..."),
 *   "name": "Laptop Pro 15",
 *   "category": {
 *     "name": "Electronics",
 *     "description": "Computers & Accessories"
 *   }
 * }
 * }</pre>
 *
 * <p>You can query embedded fields with dot notation:
 * {@code morphium.createQueryFor(Product.class).f("category.name").eq("Electronics")}.</p>
 */
// @Embedded: Tells Morphium this class is a sub-document, not an independent collection entity.
// No @Id field is needed because embedded objects are always stored within a parent document.
@Embedded
@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class Category {

    /** The category name (e.g., "Electronics"). Stored as a field within the embedded sub-document. */
    private String name;

    /** A human-readable description of the category (e.g., "Computers & Accessories"). */
    private String description;
}