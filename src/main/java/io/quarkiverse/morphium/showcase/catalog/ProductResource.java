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

import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * JAX-RS resource serving the product catalog pages.
 *
 * <p>This resource demonstrates how a Quarkus REST endpoint integrates with Morphium through the
 * {@link ProductService}. It uses Quarkus Qute templates for server-side HTML rendering and
 * showcases Morphium features including:</p>
 * <ul>
 *   <li><strong>CRUD operations</strong> -- create, read, update, delete products via Morphium</li>
 *   <li><strong>Regex search</strong> -- case-insensitive pattern matching via Morphium's query API</li>
 *   <li><strong>Range queries</strong> -- filter products by price range</li>
 *   <li><strong>Cache management</strong> -- view cache statistics and manually clear the cache</li>
 *   <li><strong>Seed data</strong> -- bulk-insert sample data with {@code morphium.storeList()}</li>
 * </ul>
 */
@Path("/catalog")
public class ProductResource {

    @Inject
    Template catalog;

    @Inject
    ProductService productService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "Object Mapping, @Entity, @Embedded"),
            new DocLink("/docs/howtos/caching-examples", "Caching Examples", "LRU Cache, Cache Invalidation"),
            new DocLink("/docs/api-reference", "API Reference", "store(), Query, delete()"),
            new DocLink("/docs/howtos/field-names", "Field Names", "@Property, @FieldNameConstants")
    );

    /**
     * Renders the main catalog page with all products, count, and cache statistics.
     * Seeds sample data on first access if the collection is empty.
     *
     * @return the rendered catalog HTML page
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        productService.seedData();
        return catalog.data("active", "catalog")
                .data("products", productService.findAll())
                .data("count", productService.count())
                .data("cacheStats", productService.getCacheStats())
                .data("searchQuery", null)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Creates a new product from form data and redirects back to the catalog page.
     * Demonstrates Morphium's {@code store()} for inserting new entities.
     *
     * @param name         the product name
     * @param description  the product description
     * @param price        the product price
     * @param stock        the stock quantity
     * @param categoryName the category name (stored as an embedded document)
     * @param categoryDesc the category description
     * @param tagsStr      comma-separated tags
     * @return a redirect response to the catalog page
     */
    @POST
    @Path("/products")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createProduct(
            @FormParam("name") String name,
            @FormParam("description") String description,
            @FormParam("price") double price,
            @FormParam("stock") int stock,
            @FormParam("categoryName") String categoryName,
            @FormParam("categoryDesc") String categoryDesc,
            @FormParam("tags") String tagsStr) {
        List<String> tags = tagsStr != null && !tagsStr.isBlank()
                ? Arrays.asList(tagsStr.split(",\\s*"))
                : List.of();
        productService.create(name, description, price, stock, categoryName, categoryDesc, tags);
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Deletes a single product by id. Demonstrates Morphium's {@code delete()} operation.
     *
     * @param id the MorphiumId string of the product to delete
     * @return a redirect response to the catalog page
     */
    @DELETE
    @Path("/products/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteProduct(@PathParam("id") String id) {
        productService.delete(id);
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Searches products by name using a regex pattern. Demonstrates Morphium's
     * {@code matches()} query operator for case-insensitive pattern matching.
     *
     * @param query the search pattern
     * @return the catalog page filtered by the search query
     */
    @POST
    @Path("/products/search")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance searchProducts(@FormParam("query") String query) {
        List<Product> results = (query != null && !query.isBlank())
                ? productService.searchByName(query)
                : productService.findAll();
        return catalog.data("active", "catalog")
                .data("products", results)
                .data("count", productService.count())
                .data("searchQuery", query)
                .data("cacheStats", productService.getCacheStats())
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Filters products by price range. Demonstrates Morphium's {@code gte()} and {@code lte()}
     * range query operators combined with sorting.
     *
     * @param min minimum price (inclusive)
     * @param max maximum price (inclusive)
     * @return the catalog page showing products within the price range
     */
    @POST
    @Path("/products/price-range")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance filterByPrice(
            @FormParam("minPrice") double min,
            @FormParam("maxPrice") double max) {
        return catalog.data("active", "catalog")
                .data("products", productService.findByPriceRange(min, max))
                .data("count", productService.count())
                .data("priceFilter", Map.of("min", min, "max", max))
                .data("cacheStats", productService.getCacheStats())
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Clears the Morphium read cache for the Product entity. Useful for demonstrating that
     * subsequent reads will fetch from MongoDB instead of the cache.
     *
     * @return a redirect response to the catalog page
     */
    @POST
    @Path("/cache/clear")
    @Produces(MediaType.TEXT_HTML)
    public Response clearCache() {
        productService.clearCache();
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Drops the entire products collection. Demonstrates Morphium's {@code dropCollection()}.
     *
     * @return a redirect response to the catalog page
     */
    @DELETE
    @Path("/products")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteAll() {
        productService.deleteAll();
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Resets the collection and re-seeds sample data. Demonstrates Morphium's
     * {@code dropCollection()} followed by {@code storeList()} for bulk inserts.
     *
     * @return a redirect response to the catalog page
     */
    @POST
    @Path("/products/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        productService.deleteAll();
        productService.seedData();
        return Response.seeOther(URI.create("/catalog")).build();
    }
}