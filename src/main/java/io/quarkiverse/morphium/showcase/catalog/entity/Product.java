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

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.driver.MorphiumId;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.FieldNameConstants;

/**
 * Morphium entity representing a product in the catalog.
 *
 * <p>This class demonstrates several core Morphium ORM features for mapping Java objects to MongoDB
 * documents:</p>
 *
 * <ul>
 *   <li><strong>{@code @Entity}</strong> -- Marks this class as a Morphium-managed entity that will be
 *       persisted in MongoDB. The {@code collectionName} attribute explicitly sets the MongoDB collection
 *       name to "products". Without it, Morphium would derive the collection name from the class name
 *       (lowercased).</li>
 *   <li><strong>{@code @Index} (class-level)</strong> -- Defines a compound index on the collection.
 *       The value {@code "-price, name"} creates an index with {@code price} in descending order and
 *       {@code name} in ascending order. Compound indexes are essential for optimizing queries that
 *       filter or sort on multiple fields.</li>
 *   <li><strong>{@code @Cache}</strong> -- Enables Morphium's built-in read cache for this entity.
 *       {@code maxEntries = 100} limits the cache to 100 entries; {@code strategy = LRU} evicts the
 *       least-recently-used entries when the cache is full; {@code timeout = 30000} invalidates cached
 *       entries after 30 seconds. Morphium automatically invalidates the cache when you write to this
 *       entity through the same Morphium instance.</li>
 *   <li><strong>{@code @WriteSafety}</strong> -- Controls the MongoDB write concern for this entity.
 *       {@code SafetyLevel.NORMAL} corresponds to acknowledged writes (w:1), meaning MongoDB confirms
 *       writes have been applied to the primary. Other levels include {@code WAIT_FOR_ALL_SLAVES}
 *       (w:majority) for stronger durability guarantees.</li>
 *   <li><strong>{@code @DefaultReadPreference}</strong> -- Specifies which replica set member to read
 *       from. {@code PRIMARY} ensures all reads go to the primary node, providing the strongest
 *       consistency. Alternatives like {@code SECONDARY_PREFERRED} allow reading from secondaries to
 *       distribute read load.</li>
 * </ul>
 *
 * <p>Lombok's {@code @FieldNameConstants} generates an inner static class {@code Fields} with string
 * constants for each field name (e.g., {@code Product.Fields.name}). These constants are used in Morphium
 * queries for type-safe field references, avoiding hard-coded strings.</p>
 */
// @Entity: Registers this class with Morphium. The collectionName explicitly maps to the "products" collection.
@Entity(collectionName = "products")
// @Index (class-level): Creates a compound index {price: -1, name: 1} for efficient price-range queries sorted by name.
@Index({"-price, name"})
// @Cache: Activates Morphium's internal read cache. After a query, results are cached for 30s (timeout).
// Up to 100 entries are kept using LRU eviction. Writes through Morphium automatically invalidate the cache.
@Cache(maxEntries = 100, strategy = Cache.ClearStrategy.LRU, timeout = 30000)
// @WriteSafety: Sets the MongoDB write concern. NORMAL = acknowledged writes (w:1).
@WriteSafety(level = SafetyLevel.NORMAL)
// @DefaultReadPreference: All reads for this entity go to the PRIMARY replica set member.
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class Product {

    /**
     * The MongoDB document {@code _id} field.
     *
     * <p>{@code @Id} tells Morphium which field maps to MongoDB's {@code _id}. The type
     * {@link MorphiumId} is Morphium's own ObjectId implementation -- it is compatible with
     * MongoDB's native ObjectId and is automatically generated on first store if left {@code null}.</p>
     */
    @Id
    private MorphiumId id;

    /**
     * The product name, indexed for fast lookups.
     *
     * <p>{@code @Index} on a single field creates a simple ascending index on "name" in MongoDB.
     * This speeds up queries that filter or sort by product name, such as
     * {@code morphium.createQueryFor(Product.class).f("name").eq("Laptop")}.</p>
     */
    @Index
    private String name;

    /**
     * A textual description of the product.
     *
     * <p>{@code @Property(fieldName = "product_description")} overrides the default field name mapping.
     * Without this annotation, Morphium would store this field as "description" in MongoDB. With it,
     * the document field is named "product_description" instead. This is useful when working with
     * existing collections that use different naming conventions than your Java code.</p>
     */
    @Property(fieldName = "product_description")
    private String description;

    /**
     * The product price, indexed for efficient range queries.
     *
     * <p>The field-level {@code @Index} creates an ascending index on "price". Combined with the
     * class-level compound index {@code {-price, name}}, this provides flexibility for queries that
     * filter by price alone or by price and name together.</p>
     */
    @Index
    private double price;

    /** The number of units currently in stock. Stored as-is in MongoDB (no special annotations needed). */
    private int stock;

    /**
     * The product's category, stored as an embedded document.
     *
     * <p>Because {@link Category} is annotated with {@code @Embedded}, Morphium stores it as a nested
     * sub-document within the Product document rather than in a separate collection. This is ideal for
     * data that "belongs" to the parent and is always accessed together with it. You can query embedded
     * fields using dot notation: {@code morphium.createQueryFor(Product.class).f("category.name").eq("Electronics")}.</p>
     */
    private Category category;

    /**
     * A list of tags associated with the product.
     *
     * <p>Morphium natively supports {@code List} fields. In MongoDB, this becomes a JSON array within
     * the document. No special annotation is required -- Morphium automatically serializes and
     * deserializes standard Java collection types.</p>
     */
    private java.util.List<String> tags;
}