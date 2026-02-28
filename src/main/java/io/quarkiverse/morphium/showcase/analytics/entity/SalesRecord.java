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
package io.quarkiverse.morphium.showcase.analytics.entity;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Morphium entity representing a sales transaction record, used to demonstrate
 * the <strong>Aggregation Framework</strong> features of Morphium ORM.
 *
 * <h3>Key Morphium Features Demonstrated</h3>
 * <ul>
 *   <li>{@code @Entity} -- Maps this class to a MongoDB collection. The {@code collectionName}
 *       attribute explicitly sets the collection name to {@code "sales_records"} instead of relying
 *       on Morphium's default naming convention (which would derive the name from the class name).</li>
 *   <li>{@code @Index} (class-level) -- Defines a <strong>compound index</strong> on multiple fields.
 *       The value {@code "region, product_name"} creates a single compound index covering both fields.
 *       This is useful for queries that filter or sort by region AND product name together.
 *       Note that Morphium uses the MongoDB field name ({@code product_name}) here, not the Java
 *       field name ({@code productName}), because Morphium's default naming strategy converts
 *       camelCase to snake_case.</li>
 *   <li>{@code @Id} -- Marks the primary key field. Morphium maps this to MongoDB's {@code _id} field.
 *       Using {@link MorphiumId} gives you a MongoDB ObjectId-compatible identifier.</li>
 *   <li>{@code @Index} (field-level) -- Creates a <strong>single-field index</strong> on the annotated
 *       field. This improves query performance for lookups by that field.</li>
 *   <li>{@code @FieldNameConstants} (Lombok) -- Generates a static inner class {@code Fields} with
 *       constants for each field name. These constants are used in type-safe Morphium queries and
 *       aggregation pipelines (e.g., {@code SalesRecord.Fields.productName}).</li>
 * </ul>
 *
 * <h3>MongoDB Field Name Mapping</h3>
 * <p>By default, Morphium converts Java camelCase field names to snake_case for MongoDB storage.
 * For example, {@code productName} becomes {@code product_name}, {@code unitPrice} becomes
 * {@code unit_price}, and {@code saleDate} becomes {@code sale_date}. This is important to
 * remember when writing raw aggregation expressions that reference field names with {@code $}
 * notation (e.g., {@code "$product_name"}, {@code "$unit_price"}).</p>
 *
 * @see io.quarkiverse.morphium.showcase.analytics.AnalyticsService AnalyticsService for aggregation queries on this entity
 */
// @Entity -- Tells Morphium this class is a MongoDB document. The collectionName parameter
// overrides the default collection name that Morphium would derive from the class name.
@Entity(collectionName = "sales_records")

// @Index at class level -- Defines a compound index on (region, product_name).
// Compound indexes support queries that filter on both fields together or on the
// "prefix" of the index (i.e., region alone). The field names here use MongoDB's
// snake_case convention because that is what Morphium writes to the database.
@Index({"region, product_name"})

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class SalesRecord {

    /**
     * The unique document identifier, mapped to MongoDB's {@code _id} field.
     *
     * <p>{@code @Id} is mandatory for every Morphium entity. Morphium uses this field to
     * identify documents for update and delete operations. If left {@code null} when storing,
     * Morphium (or MongoDB) will auto-generate an ObjectId.</p>
     *
     * <p>{@link MorphiumId} is Morphium's own ObjectId implementation, compatible with
     * MongoDB's native {@code ObjectId}. You could also use {@code String} or
     * {@code org.bson.types.ObjectId} as the ID type.</p>
     */
    @Id
    private MorphiumId id;

    /**
     * The name of the product sold.
     *
     * <p>{@code @Index} at the field level creates a <strong>single-field ascending index</strong>
     * in MongoDB on this field. This is the simplest way to add an index -- just annotate
     * the field. For compound indexes spanning multiple fields, use {@code @Index} at the
     * class level instead (as shown above).</p>
     *
     * <p>Morphium will store this as {@code product_name} in MongoDB due to its default
     * camelCase-to-snake_case name mapping.</p>
     */
    @Index
    private String productName;

    /**
     * The price per unit of the product. Stored as {@code unit_price} in MongoDB.
     * Used in aggregation pipelines to calculate revenue via {@code $multiply}.
     */
    private double unitPrice;

    /**
     * The number of units sold in this transaction.
     * Multiplied with {@code unitPrice} in aggregation pipelines to compute total revenue.
     */
    private int quantity;

    /**
     * The discount percentage applied (0.0 to 1.0 range).
     * Included to show that not all fields need annotations -- plain fields are automatically
     * persisted by Morphium with their snake_case name.
     */
    private double discount;

    /**
     * The sales region (e.g., "Europe", "North America").
     * Part of the class-level compound index {@code (region, product_name)}.
     * Used as the grouping key in the {@code salesByRegion()} aggregation.
     */
    private String region;

    /**
     * The name of the sales representative who closed the deal.
     * Used as the grouping key in the {@code topSalesReps()} aggregation.
     */
    private String salesRep;

    /**
     * The date and time of the sale.
     *
     * <p>{@code @Index} creates a single-field index, enabling efficient time-range queries
     * and sorting by date. Morphium handles {@link LocalDateTime} serialization to MongoDB's
     * Date type via built-in type mappers (specifically {@code LocalDateTimeMapper}).</p>
     */
    @Index
    private LocalDateTime saleDate;

    /**
     * A list of category tags for the product (e.g., ["electronics"]).
     *
     * <p>Morphium automatically serializes {@code List<String>} as a BSON array in MongoDB.
     * No special annotation is needed for simple collections of primitives or strings.</p>
     */
    private List<String> categories;
}