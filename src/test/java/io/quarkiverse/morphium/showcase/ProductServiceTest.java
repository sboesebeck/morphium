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
package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.catalog.ProductService;
import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ProductService} -- a product catalog backed by Morphium/MongoDB.
 *
 * <h2>How This Test Works</h2>
 * <p>
 * The {@code @QuarkusTest} annotation starts the full Quarkus CDI container, making all application
 * beans available via {@code @Inject}. The test-scoped {@code application.properties} configures
 * Morphium to use the {@code InMemDriver}, so <strong>no external MongoDB instance or Docker container
 * is needed</strong>.
 * </p>
 *
 * <h2>InMemDriver Configuration (src/test/resources/application.properties)</h2>
 * <pre>
 *   quarkus.morphium.database=inmem-test
 *   quarkus.morphium.driver-name=InMemDriver
 *   quarkus.morphium.devservices.enabled=false
 * </pre>
 * <p>
 * The InMemDriver simulates MongoDB in-process. It supports the full Morphium query API including
 * regex matching, range queries, sorting, and more. This makes it ideal for testing service-layer
 * logic without infrastructure overhead.
 * </p>
 *
 * <h2>What This Test Covers</h2>
 * <ul>
 *   <li>CRUD operations: create, read (by id, by name, by price range), delete</li>
 *   <li>Embedded objects: the {@link Product} entity contains a nested {@code Category} object,
 *       demonstrating how Morphium handles embedded document mapping</li>
 *   <li>Collection count via {@code morphium.createQueryFor(...).countAll()}</li>
 *   <li>Idempotent seed data: verifying that calling {@code seedData()} twice does not duplicate records</li>
 *   <li>Regex-based text search using Morphium's {@code .matches()} query operator</li>
 * </ul>
 *
 * @see ProductService
 * @see de.caluga.morphium.driver.inmem.InMemoryDriver
 */
@QuarkusTest
class ProductServiceTest {

    // The ProductService is the system under test -- injected by Quarkus CDI.
    @Inject
    ProductService productService;

    // Direct access to the Morphium instance for test setup/teardown operations
    // that are outside the service's responsibility (e.g., dropping collections).
    @Inject
    Morphium morphium;

    /**
     * Cleans up the Product collection before each test to ensure isolation.
     * <p>
     * {@code morphium.dropCollection(Product.class)} removes all documents <em>and</em> any
     * indexes defined on the collection. With InMemDriver this is instantaneous. For tests
     * that depend on specific indexes, call {@code morphium.ensureIndicesFor(Product.class)}
     * after the drop.
     * </p>
     */
    @BeforeEach
    void setUp() {
        // Drop the entire collection for a clean slate.
        // This is preferred over deleteMany() because it also resets indexes.
        morphium.dropCollection(Product.class);
    }

    /**
     * Verifies the full create-then-find lifecycle of a product entity.
     * <p>
     * Demonstrates several Morphium concepts:
     * <ul>
     *   <li><strong>Auto-generated ID</strong>: After {@code morphium.store()}, the entity's
     *       {@code id} field is populated with a {@link de.caluga.morphium.driver.MorphiumId}.</li>
     *   <li><strong>Lookup by ID</strong>: Uses {@code .f(Product.Fields.id).eq(new MorphiumId(id))}
     *       to query by the primary key. The generated {@code Fields} inner class provides
     *       type-safe field name references.</li>
     *   <li><strong>Embedded documents</strong>: The {@code Category} object is stored as a nested
     *       BSON document inside the Product document, and Morphium deserializes it automatically.</li>
     * </ul>
     * </p>
     */
    @Test
    void shouldCreateAndFindProduct() {
        // Create a product with an embedded Category object
        Product product = productService.create(
                "Test Laptop", "A test laptop", 999.99, 10,
                "Electronics", "Computers", List.of("laptop", "test")
        );

        // After store(), Morphium assigns a MorphiumId to the entity
        assertThat(product.getId()).isNotNull();

        // Find the product by its generated id (converted to String and back to MorphiumId)
        Product found = productService.findById(product.getId().toString());
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Test Laptop");
        assertThat(found.getPrice()).isEqualTo(999.99);

        // Verify the embedded Category object was persisted and deserialized correctly
        assertThat(found.getCategory()).isNotNull();
        assertThat(found.getCategory().getName()).isEqualTo("Electronics");
    }

    /**
     * Tests range queries using Morphium's {@code .gte()} and {@code .lte()} operators.
     * <p>
     * This demonstrates how to build compound queries in Morphium. The service chains
     * two filter conditions on the same field:
     * <pre>
     *   morphium.createQueryFor(Product.class)
     *       .f(Product.Fields.price).gte(min)
     *       .f(Product.Fields.price).lte(max)
     *       .sort(Map.of(Product.Fields.price, 1))
     *       .asList();
     * </pre>
     * Multiple {@code .f()} calls on the same query act as an AND conjunction.
     * </p>
     */
    @Test
    void shouldFindByPriceRange() {
        // Create three products at different price points
        productService.create("Cheap", "Cheap item", 10.0, 5, "Cat", "Desc", List.of());
        productService.create("Mid", "Mid item", 50.0, 5, "Cat", "Desc", List.of());
        productService.create("Expensive", "Expensive item", 200.0, 5, "Cat", "Desc", List.of());

        // Query for products between 20.0 and 100.0 -- only "Mid" (50.0) should match
        List<Product> results = productService.findByPriceRange(20.0, 100.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Mid");
    }

    /**
     * Tests regex-based text search using Morphium's {@code .matches()} operator.
     * <p>
     * Under the hood, the service uses:
     * <pre>
     *   morphium.createQueryFor(Product.class)
     *       .f(Product.Fields.name).matches("(?i)" + pattern)
     *       .asList();
     * </pre>
     * The {@code (?i)} prefix makes the regex case-insensitive. This maps to MongoDB's
     * {@code $regex} operator. The InMemDriver supports regex queries, making it possible
     * to test this pattern without a real database.
     * </p>
     */
    @Test
    void shouldSearchByName() {
        productService.create("Laptop Pro", "Desc", 999.0, 1, "C", "D", List.of());
        productService.create("Mouse Pad", "Desc", 9.0, 1, "C", "D", List.of());

        // Search using a partial name -- matches "Laptop Pro" via regex
        List<Product> results = productService.searchByName("Laptop");
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Laptop Pro");
    }

    /**
     * Tests the {@code countAll()} query method which maps to MongoDB's {@code count} command.
     * <p>
     * This verifies that {@code morphium.createQueryFor(Product.class).countAll()} returns
     * the correct document count. It is tested both on an empty collection and after inserts
     * to confirm accuracy.
     * </p>
     */
    @Test
    void shouldCountProducts() {
        // Empty collection should return zero
        assertThat(productService.count()).isZero();

        // Insert two products and verify the count
        productService.create("P1", "D", 10.0, 1, "C", "D", List.of());
        productService.create("P2", "D", 20.0, 1, "C", "D", List.of());

        assertThat(productService.count()).isEqualTo(2);
    }

    /**
     * Tests entity deletion via {@code morphium.delete(entity)}.
     * <p>
     * Morphium's {@code delete()} method removes a single document identified by its
     * {@code _id} field. We verify deletion by checking that the collection count drops
     * back to zero. This is a straightforward way to confirm delete operations in tests.
     * </p>
     */
    @Test
    void shouldDeleteProduct() {
        Product product = productService.create("ToDelete", "D", 10.0, 1, "C", "D", List.of());
        assertThat(productService.count()).isEqualTo(1);

        // Delete by id -- internally calls morphium.delete(product)
        productService.delete(product.getId().toString());

        // Verify the collection is now empty
        assertThat(productService.count()).isZero();
    }

    /**
     * Tests the idempotent seed data mechanism.
     * <p>
     * The {@code seedData()} method first checks whether the collection already contains
     * data ({@code if (count() > 0) return;}). This is a common pattern for initializing
     * demo or reference data without duplicating it on repeated calls.
     * </p>
     * <p>
     * We verify two things:
     * <ol>
     *   <li>After the first call, the collection is populated (count &gt; 0).</li>
     *   <li>After a second call, the count remains unchanged (idempotency).</li>
     * </ol>
     * </p>
     */
    @Test
    void shouldSeedData() {
        // First seed: should populate the collection
        productService.seedData();
        assertThat(productService.count()).isGreaterThan(0);

        // Second seed: should be a no-op (idempotent guard)
        long countBefore = productService.count();
        productService.seedData();
        assertThat(productService.count()).isEqualTo(countBefore);
    }
}
