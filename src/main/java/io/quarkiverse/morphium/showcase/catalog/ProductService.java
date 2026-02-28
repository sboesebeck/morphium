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
package io.quarkiverse.morphium.showcase.catalog;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.catalog.entity.Category;
import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ProductService {

    @Inject
    Morphium morphium;

    public void ensureIndexes() {
        morphium.ensureIndicesFor(Product.class);
    }

    public List<Product> findAll() {
        return morphium.createQueryFor(Product.class).asList();
    }

    public Product findById(String id) {
        return morphium.createQueryFor(Product.class)
                .f(Product.Fields.id).eq(new MorphiumId(id))
                .get();
    }

    public List<Product> findByName(String name) {
        return morphium.createQueryFor(Product.class)
                .f(Product.Fields.name).eq(name)
                .asList();
    }

    public List<Product> searchByName(String pattern) {
        return morphium.createQueryFor(Product.class)
                .f(Product.Fields.name).matches("(?i)" + pattern)
                .asList();
    }

    public List<Product> findByPriceRange(double min, double max) {
        return morphium.createQueryFor(Product.class)
                .f(Product.Fields.price).gte(min)
                .f(Product.Fields.price).lte(max)
                .sort(Map.of(Product.Fields.price, 1))
                .asList();
    }

    public List<Product> findByCategory(String categoryName) {
        return morphium.createQueryFor(Product.class)
                .f("category.name").eq(categoryName)
                .asList();
    }

    public long count() {
        return morphium.createQueryFor(Product.class).countAll();
    }

    public Product store(Product product) {
        morphium.store(product);
        return product;
    }

    public Product create(String name, String description, double price, int stock,
                          String categoryName, String categoryDesc, List<String> tags) {
        Product product = Product.builder()
                .name(name)
                .description(description)
                .price(price)
                .stock(stock)
                .category(new Category(categoryName, categoryDesc))
                .tags(tags)
                .build();
        morphium.store(product);
        return product;
    }

    public void delete(String id) {
        Product product = findById(id);
        if (product != null) {
            morphium.delete(product);
        }
    }

    public void deleteAll() {
        morphium.dropCollection(Product.class);
    }

    public Product reread(Product product) {
        morphium.reread(product);
        return product;
    }

    public Map<String, Object> getCacheStats() {
        var stats = morphium.getStatistics();
        var cacheSizes = morphium.getCache().getSizes();
        return Map.of(
                "cacheSizes", cacheSizes,
                "statistics", stats
        );
    }

    public void clearCache() {
        morphium.getCache().clearCachefor(Product.class);
    }

    public List<Product> findWithPagination(int skip, int limit, String sortField, int sortDir) {
        return morphium.createQueryFor(Product.class)
                .sort(Map.of(sortField, sortDir))
                .skip(skip)
                .limit(limit)
                .asList();
    }

    public void seedData() {
        if (count() > 0) return;

        List<Product> products = List.of(
                Product.builder().name("Laptop Pro 15").description("High-performance laptop").price(1299.99).stock(50)
                        .category(new Category("Electronics", "Computers & Accessories")).tags(List.of("laptop", "pro", "portable")).build(),
                Product.builder().name("Wireless Mouse").description("Ergonomic wireless mouse").price(29.99).stock(200)
                        .category(new Category("Electronics", "Peripherals")).tags(List.of("mouse", "wireless", "ergonomic")).build(),
                Product.builder().name("Standing Desk").description("Adjustable height standing desk").price(449.00).stock(30)
                        .category(new Category("Furniture", "Office")).tags(List.of("desk", "standing", "ergonomic")).build(),
                Product.builder().name("Mechanical Keyboard").description("Cherry MX Blue switches").price(89.99).stock(120)
                        .category(new Category("Electronics", "Peripherals")).tags(List.of("keyboard", "mechanical", "gaming")).build(),
                Product.builder().name("Monitor 27\"").description("4K IPS display").price(399.99).stock(75)
                        .category(new Category("Electronics", "Displays")).tags(List.of("monitor", "4k", "ips")).build(),
                Product.builder().name("USB-C Hub").description("7-in-1 USB-C adapter").price(49.99).stock(300)
                        .category(new Category("Electronics", "Accessories")).tags(List.of("usb-c", "hub", "adapter")).build(),
                Product.builder().name("Office Chair").description("Mesh back ergonomic chair").price(299.00).stock(45)
                        .category(new Category("Furniture", "Office")).tags(List.of("chair", "ergonomic", "office")).build(),
                Product.builder().name("Webcam HD").description("1080p webcam with mic").price(59.99).stock(150)
                        .category(new Category("Electronics", "Accessories")).tags(List.of("webcam", "hd", "streaming")).build()
        );
        morphium.storeList(products);
    }
}