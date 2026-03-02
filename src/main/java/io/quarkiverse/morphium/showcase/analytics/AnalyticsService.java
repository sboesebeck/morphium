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
package io.quarkiverse.morphium.showcase.analytics;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import io.quarkiverse.morphium.showcase.analytics.entity.SalesRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service demonstrating Morphium's <strong>Aggregation Framework</strong> API.
 *
 * <p>MongoDB's aggregation framework is one of its most powerful features, allowing you to
 * perform complex data transformations and analytics directly in the database. Morphium
 * provides a Java-friendly builder API through the {@link Aggregator} class, which maps
 * closely to MongoDB's native aggregation pipeline stages.</p>
 *
 * <h3>How Morphium Aggregation Works</h3>
 * <ol>
 *   <li>Create an {@link Aggregator} via {@code morphium.createAggregator(InputType.class, OutputType.class)}.
 *       The first type parameter is the source entity (determines the collection), and the second
 *       is the result type (often {@code Map} for flexible results).</li>
 *   <li>Chain pipeline stages: {@code group()}, {@code project()}, {@code match()}, {@code sort()},
 *       {@code limit()}, {@code unwind()}, etc.</li>
 *   <li>Within a {@code group()} stage, use accumulators: {@code sum()}, {@code avg()},
 *       {@code min()}, {@code max()}, {@code first()}, {@code last()}, etc.</li>
 *   <li>Call {@code aggregate()} to execute the pipeline and return results.</li>
 * </ol>
 *
 * <h3>Important: MongoDB Field Names vs Java Field Names</h3>
 * <p>In aggregation expressions, you must use MongoDB's stored field names (snake_case by default),
 * not Java field names. For example, use {@code "$unit_price"} (not {@code "$unitPrice"}) and
 * {@code "$product_name"} (not {@code "$productName"}) because Morphium's default
 * {@code NameProvider} converts camelCase to snake_case.</p>
 *
 * <p>This service also demonstrates:</p>
 * <ul>
 *   <li>{@code morphium.createQueryFor()} -- Creating type-safe queries</li>
 *   <li>{@code query.countAll()} -- Counting documents</li>
 *   <li>{@code query.distinct()} -- Retrieving distinct values for a field</li>
 *   <li>{@code morphium.storeList()} -- Bulk-inserting multiple documents at once</li>
 * </ul>
 *
 * @see SalesRecord the entity this service queries
 * @see Aggregator Morphium's aggregation pipeline builder
 */
@ApplicationScoped
public class AnalyticsService {

    /**
     * The main Morphium instance, injected by the Quarkus CDI container via the
     * quarkus-morphium extension. This single instance manages connection pooling,
     * caching, and all ORM operations.
     */
    @Inject
    Morphium morphium;

    /**
     * Aggregates sales data grouped by region, computing total revenue, order count,
     * and average order value for each region.
     *
     * <p>This demonstrates a {@code $group} stage with multiple accumulators and a
     * computed expression ({@code $multiply}) for calculating derived values.</p>
     *
     * @return a list of maps, each containing {@code _id} (region name), {@code totalRevenue},
     *         {@code totalOrders}, and {@code avgOrderValue}, sorted by revenue descending
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> salesByRegion() {
        // createAggregator() -- Creates a new aggregation pipeline builder.
        // First param: the source entity class (determines which MongoDB collection to aggregate).
        // Second param: the output type. Using Map.class gives flexible access to result fields.
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);

        // group("$region") -- Starts a $group stage with "region" as the grouping key (_id).
        // The "$" prefix denotes a MongoDB field reference. Must use the MongoDB field name (snake_case).
        agg.group("$region")
                // sum("totalRevenue", expr) -- Accumulates the sum of (unit_price * quantity) for each group.
                // The Map.of("$multiply", ...) creates a MongoDB $multiply expression inline.
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                // sum("totalOrders", 1) -- Counts documents by summing 1 for each document in the group.
                .sum("totalOrders", 1)
                // avg() -- Computes the average of the given expression across the group.
                .avg("avgOrderValue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                // end() -- Finalizes the $group stage and returns to the pipeline builder.
                .end();

        // sort() -- Adds a $sort stage. Use -1 for descending, 1 for ascending.
        agg.sort(Map.of("totalRevenue", -1));

        // aggregate() -- Executes the pipeline against MongoDB and returns the results.
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    /**
     * Aggregates sales data grouped by product, returning the top 10 products by revenue.
     *
     * <p>Demonstrates using {@code $group} with field references as accumulators
     * (e.g., {@code "$quantity"} directly sums the quantity field) and the {@code limit()}
     * pipeline stage to restrict results.</p>
     *
     * @return a list of maps with {@code _id} (product name), {@code totalQuantity},
     *         {@code totalRevenue}, and {@code avgPrice}, limited to top 10
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> salesByProduct() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);

        // Group by product_name (the MongoDB field name for "productName").
        agg.group("$product_name")
                // sum("totalQuantity", "$quantity") -- Sums the "quantity" field directly.
                // When the second argument is a String starting with "$", it references a field.
                .sum("totalQuantity", "$quantity")
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                // avg("avgPrice", "$unit_price") -- Averages the unit_price field across the group.
                .avg("avgPrice", "$unit_price")
                .end();
        agg.sort(Map.of("totalRevenue", -1));

        // limit(10) -- Adds a $limit stage, restricting the output to the first 10 documents.
        // Combined with the preceding $sort, this gives us "top 10 by revenue".
        agg.limit(10);
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    /**
     * Aggregates sales data by month and year, demonstrating the {@code $project} stage
     * combined with date extraction operators.
     *
     * <p>The pipeline first uses {@code $project} to extract month and year from the sale date
     * and compute per-document revenue, then {@code $group}s by the compound key
     * {@code {year, month}}.</p>
     *
     * @return a list of maps with {@code _id} containing {@code year} and {@code month},
     *         plus {@code totalRevenue} and {@code totalOrders}, sorted chronologically
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> monthlySales() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);

        // project() -- Adds a $project stage that reshapes each document.
        // Here we extract "month" and "year" from the sale_date field using MongoDB's
        // $month and $year date operators, and compute revenue per document.
        // This is useful because $group can then use these projected fields.
        agg.project(Map.of(
                "month", Map.of("$month", "$sale_date"),
                "year", Map.of("$year", "$sale_date"),
                "revenue", Map.of("$multiply", List.of("$unit_price", "$quantity"))
        ));

        // group() with a compound key -- When you pass a Map as the grouping expression,
        // MongoDB creates a compound _id with multiple fields. Each document is grouped
        // by the combination of year AND month.
        agg.group(Map.of("year", "$year", "month", "$month"))
                .sum("totalRevenue", "$revenue")
                .sum("totalOrders", 1)
                .end();

        // Sort by the nested _id fields: first by year ascending, then by month ascending.
        // Access nested group-key fields with dot notation: "_id.year", "_id.month".
        agg.sort(Map.of("_id.year", 1, "_id.month", 1));
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    /**
     * Finds the top 5 sales representatives by total revenue.
     *
     * <p>Simple {@code $group} + {@code $sort} + {@code $limit} pipeline, a very common
     * pattern for "top N" queries in MongoDB.</p>
     *
     * @return a list of maps with {@code _id} (sales rep name), {@code totalSales} (count),
     *         and {@code totalRevenue}, limited to the top 5
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> topSalesReps() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);
        // Group by the sales_rep field (snake_case MongoDB name for "salesRep").
        agg.group("$sales_rep")
                .sum("totalSales", 1)
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                .end();
        agg.sort(Map.of("totalRevenue", -1));
        agg.limit(5);
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    /**
     * Returns the total number of sales records in the collection.
     *
     * <p>Demonstrates {@code morphium.createQueryFor(Class).countAll()} -- the simplest way
     * to count all documents in a collection. Under the hood, Morphium translates this to
     * MongoDB's {@code countDocuments()} command.</p>
     *
     * @return the document count for the {@code sales_records} collection
     */
    public long totalRecords() {
        // createQueryFor() -- Creates a type-safe query object for the given entity class.
        // countAll() -- Executes a count command (no filter) and returns the total document count.
        return morphium.createQueryFor(SalesRecord.class).countAll();
    }

    /**
     * Computes the total revenue across all regions by summing up the region-level aggregation.
     *
     * <p>This is a convenience method that reuses the {@code salesByRegion()} aggregation
     * and sums the results in Java. An alternative would be a single aggregation pipeline
     * with no grouping key (i.e., {@code group(null)}) to get a grand total.</p>
     *
     * @return the sum of {@code totalRevenue} across all regions
     */
    public double totalRevenue() {
        List<Map<String, Object>> result = salesByRegion();
        return result.stream()
                .mapToDouble(m -> ((Number) m.getOrDefault("totalRevenue", 0)).doubleValue())
                .sum();
    }

    /**
     * Returns all distinct region values from the sales records.
     *
     * <p>Demonstrates {@code query.distinct(fieldName)} -- Morphium's wrapper around
     * MongoDB's {@code distinct} command. The field name must be the MongoDB field name
     * (snake_case). This returns a list of unique values for the specified field.</p>
     *
     * @return a list of unique region strings (e.g., ["Europe", "North America", ...])
     */
    @SuppressWarnings("unchecked")
    public List<String> distinctRegions() {
        // distinct("region") -- Retrieves all unique values for the "region" field.
        // Note: uses the MongoDB field name. Since "region" is already lowercase with
        // no camelCase, it stays the same.
        return (List<String>) (List<?>) morphium.createQueryFor(SalesRecord.class)
                .distinct("region");
    }

    /**
     * Returns all distinct product names from the sales records.
     *
     * @return a list of unique product name strings
     */
    @SuppressWarnings("unchecked")
    public List<String> distinctProducts() {
        // distinct("product_name") -- Uses the MongoDB field name (snake_case), not
        // the Java field name "productName". This is a common gotcha when using Morphium's
        // distinct() with the default NameProvider.
        return (List<String>) (List<?>) morphium.createQueryFor(SalesRecord.class)
                .distinct("product_name");
    }

    /**
     * Seeds the database with 200 sample sales records for demonstration purposes.
     *
     * <p>Demonstrates two key Morphium operations:</p>
     * <ul>
     *   <li>{@code createQueryFor().countAll()} -- Check if data already exists (idempotent seeding)</li>
     *   <li>{@code morphium.storeList()} -- Bulk-insert a list of entities in a single operation.
     *       This is significantly more efficient than calling {@code store()} individually for each
     *       document, as it sends a single bulk write command to MongoDB.</li>
     * </ul>
     *
     * <p>Note that the entities are built without setting the {@code id} field. When Morphium
     * stores them, MongoDB will auto-generate an ObjectId for each document's {@code _id} field,
     * and Morphium will populate the Java object's {@code id} field with the generated value.</p>
     */
    public void resetData() {
        morphium.dropCollection(SalesRecord.class);
        seedData();
    }

    public void seedData() {
        // Guard: only seed if the collection is empty. This makes the seed operation idempotent.
        if (morphium.createQueryFor(SalesRecord.class).countAll() > 0) return;

        String[] products = {"Laptop Pro 15", "Wireless Mouse", "Standing Desk", "Mechanical Keyboard", "Monitor 27\"", "USB-C Hub"};
        double[] prices = {1299.99, 29.99, 449.00, 89.99, 399.99, 49.99};
        String[] regions = {"Europe", "North America", "Asia Pacific", "Latin America"};
        String[] reps = {"Alice", "Bob", "Charlie", "Diana", "Eve"};

        java.util.Random rand = new java.util.Random(42);
        ArrayList<SalesRecord> records = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            int pi = rand.nextInt(products.length);
            records.add(SalesRecord.builder()
                    .productName(products[pi])
                    .unitPrice(prices[pi])
                    .quantity(1 + rand.nextInt(10))
                    .discount(rand.nextDouble() * 0.2)
                    .region(regions[rand.nextInt(regions.length)])
                    .salesRep(reps[rand.nextInt(reps.length)])
                    .saleDate(LocalDateTime.now().minusDays(rand.nextInt(365)))
                    .categories(List.of("electronics"))
                    .build());
        }

        // storeList() -- Bulk-stores all entities in a single operation. Morphium sends this
        // as a bulk insert to MongoDB, which is much faster than individual store() calls.
        // After this call, each SalesRecord's "id" field will be populated with the
        // auto-generated MorphiumId.
        morphium.storeList(records);
    }
}