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
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
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
    @Location("tags/learn-catalog.html")
    Template learnCatalog;

    @Inject
    @Location("tags/demo-catalog.html")
    Template demoCatalog;

    @Inject
    ProductService productService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "Object Mapping, @Entity, @Embedded"),
            new DocLink("/docs/howtos/caching-examples", "Caching Examples", "LRU Cache, Cache Invalidation"),
            new DocLink("/docs/api-reference", "API Reference", "store(), Query, delete()"),
            new DocLink("/docs/howtos/field-names", "Field Names", "@Property, @FieldNameConstants")
    );

    /**
     * Renders the main catalog page with the Learn tab as default.
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        productService.seedData();
        return catalog.data("active", "catalog")
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Returns the Learn tab partial (for HTMX).
     */
    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnCatalog.data("docLinks", DOC_LINKS);
    }

    /**
     * Returns the Demo tab partial (for HTMX).
     */
    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        productService.seedData();
        return demoCatalog.data("products", productService.findAll())
                .data("count", productService.count())
                .data("searchQuery", null)
                .data("successMessage", null)
                .data("errorMessage", null);
    }

    private TemplateInstance demoTabWithMessage(String successMessage, String errorMessage) {
        return demoCatalog.data("products", productService.findAll())
                .data("count", productService.count())
                .data("searchQuery", null)
                .data("successMessage", successMessage)
                .data("errorMessage", errorMessage);
    }

    private TemplateInstance demoTabWithProducts(List<Product> products, String searchQuery) {
        return demoCatalog.data("products", products)
                .data("count", productService.count())
                .data("searchQuery", searchQuery)
                .data("successMessage", null)
                .data("errorMessage", null);
    }

    private boolean isHtmxRequest(HttpHeaders headers) {
        return headers.getHeaderString("HX-Request") != null;
    }

    /**
     * Creates a new product from form data.
     * HTMX: returns demo tab partial with success message.
     * Non-HTMX: redirects to catalog page.
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
            @FormParam("tags") String tagsStr,
            @Context HttpHeaders headers) {
        List<String> tags = tagsStr != null && !tagsStr.isBlank()
                ? Arrays.asList(tagsStr.split(",\\s*"))
                : List.of();
        productService.create(name, description, price, stock, categoryName, categoryDesc, tags);
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithMessage("Product '" + name + "' created successfully.", null)).build();
        }
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Deletes a single product by id.
     * HTMX: returns demo tab partial with success message.
     * Non-HTMX: redirects to catalog page.
     */
    @DELETE
    @Path("/products/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteProduct(@PathParam("id") String id, @Context HttpHeaders headers) {
        productService.delete(id);
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithMessage("Product deleted.", null)).build();
        }
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Searches products by name using a regex pattern.
     * HTMX: returns demo tab partial with filtered results.
     * Non-HTMX: returns full catalog page.
     */
    @POST
    @Path("/products/search")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response searchProducts(@FormParam("query") String query, @Context HttpHeaders headers) {
        List<Product> results = (query != null && !query.isBlank())
                ? productService.searchByName(query)
                : productService.findAll();
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithProducts(results, query)).build();
        }
        return Response.ok(catalog.data("active", "catalog").data("docLinks", DOC_LINKS)).build();
    }

    /**
     * Filters products by price range.
     * HTMX: returns demo tab partial with filtered results.
     * Non-HTMX: returns full catalog page.
     */
    @POST
    @Path("/products/price-range")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response filterByPrice(
            @FormParam("minPrice") double min,
            @FormParam("maxPrice") double max,
            @Context HttpHeaders headers) {
        List<Product> results = productService.findByPriceRange(min, max);
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithProducts(results, null)).build();
        }
        return Response.ok(catalog.data("active", "catalog").data("docLinks", DOC_LINKS)).build();
    }

    /**
     * Clears the Morphium read cache for the Product entity.
     * HTMX: returns demo tab partial with success message.
     * Non-HTMX: redirects to catalog page.
     */
    @POST
    @Path("/cache/clear")
    @Produces(MediaType.TEXT_HTML)
    public Response clearCache(@Context HttpHeaders headers) {
        productService.clearCache();
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithMessage("Cache cleared for Product entity.", null)).build();
        }
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Drops the entire products collection.
     */
    @DELETE
    @Path("/products")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteAll(@Context HttpHeaders headers) {
        productService.deleteAll();
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithMessage("All products deleted.", null)).build();
        }
        return Response.seeOther(URI.create("/catalog")).build();
    }

    /**
     * Resets the collection and re-seeds sample data.
     * HTMX: returns demo tab partial with success message.
     * Non-HTMX: redirects to catalog page.
     */
    @POST
    @Path("/products/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        productService.deleteAll();
        productService.seedData();
        if (isHtmxRequest(headers)) {
            return Response.ok(demoTabWithMessage("Sample data re-seeded.", null)).build();
        }
        return Response.seeOther(URI.create("/catalog")).build();
    }
}
