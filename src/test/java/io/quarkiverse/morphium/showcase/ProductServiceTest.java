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

@QuarkusTest
class ProductServiceTest {

    @Inject
    ProductService productService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Product.class);
    }

    @Test
    void shouldCreateAndFindProduct() {
        Product product = productService.create(
                "Test Laptop", "A test laptop", 999.99, 10,
                "Electronics", "Computers", List.of("laptop", "test")
        );

        assertThat(product.getId()).isNotNull();

        Product found = productService.findById(product.getId().toString());
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Test Laptop");
        assertThat(found.getPrice()).isEqualTo(999.99);
        assertThat(found.getCategory()).isNotNull();
        assertThat(found.getCategory().getName()).isEqualTo("Electronics");
    }

    @Test
    void shouldFindByPriceRange() {
        productService.create("Cheap", "Cheap item", 10.0, 5, "Cat", "Desc", List.of());
        productService.create("Mid", "Mid item", 50.0, 5, "Cat", "Desc", List.of());
        productService.create("Expensive", "Expensive item", 200.0, 5, "Cat", "Desc", List.of());

        List<Product> results = productService.findByPriceRange(20.0, 100.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Mid");
    }

    @Test
    void shouldSearchByName() {
        productService.create("Laptop Pro", "Desc", 999.0, 1, "C", "D", List.of());
        productService.create("Mouse Pad", "Desc", 9.0, 1, "C", "D", List.of());

        List<Product> results = productService.searchByName("Laptop");
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Laptop Pro");
    }

    @Test
    void shouldCountProducts() {
        assertThat(productService.count()).isZero();

        productService.create("P1", "D", 10.0, 1, "C", "D", List.of());
        productService.create("P2", "D", 20.0, 1, "C", "D", List.of());

        assertThat(productService.count()).isEqualTo(2);
    }

    @Test
    void shouldDeleteProduct() {
        Product product = productService.create("ToDelete", "D", 10.0, 1, "C", "D", List.of());
        assertThat(productService.count()).isEqualTo(1);

        productService.delete(product.getId().toString());
        assertThat(productService.count()).isZero();
    }

    @Test
    void shouldSeedData() {
        productService.seedData();
        assertThat(productService.count()).isGreaterThan(0);

        // Calling seed again should not duplicate
        long countBefore = productService.count();
        productService.seedData();
        assertThat(productService.count()).isEqualTo(countBefore);
    }
}