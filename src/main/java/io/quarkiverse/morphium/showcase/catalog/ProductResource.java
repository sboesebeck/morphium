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

    @DELETE
    @Path("/products/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteProduct(@PathParam("id") String id) {
        productService.delete(id);
        return Response.seeOther(URI.create("/catalog")).build();
    }

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

    @POST
    @Path("/cache/clear")
    @Produces(MediaType.TEXT_HTML)
    public Response clearCache() {
        productService.clearCache();
        return Response.seeOther(URI.create("/catalog")).build();
    }

    @DELETE
    @Path("/products")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteAll() {
        productService.deleteAll();
        return Response.seeOther(URI.create("/catalog")).build();
    }

    @POST
    @Path("/products/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        productService.deleteAll();
        productService.seedData();
        return Response.seeOther(URI.create("/catalog")).build();
    }
}