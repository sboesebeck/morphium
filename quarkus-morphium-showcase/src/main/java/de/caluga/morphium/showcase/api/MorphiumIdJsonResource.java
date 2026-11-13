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
package de.caluga.morphium.showcase.api;

import de.caluga.morphium.showcase.catalog.ProductService;
import de.caluga.morphium.showcase.catalog.entity.Product;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;

/**
 * Self-contained demonstration of the {@code quarkus-morphium} extension's automatic
 * {@code MorphiumId} JSON (de)serialization -- a feature documented in
 * {@code morphium/docs/quarkus-extension.md} ("MorphiumId JSON serialization").
 *
 * <h2>The feature</h2>
 * <p>Every Morphium entity's {@code @Id} field is typed {@code MorphiumId}
 * ({@code de.caluga.morphium.driver.MorphiumId}), Morphium's own ObjectId implementation. Left to
 * its own devices, a generic JSON mapper (Jackson or JSON-B) would walk {@code MorphiumId}'s bean
 * getters -- {@code getPid()}, {@code getCounter()}, {@code getMachineId()}, {@code getBytes()},
 * {@code getTime()} -- and serialize the field as a nested struct, e.g.:
 * <pre>{@code "id": {"pid": 12345, "counter": 1, "machineId": 987, "bytes": "...", "time": 1700000000000}}</pre>
 * That internal shape leaks implementation details, breaks naive client-side row-identity checks
 * (e.g. {@code String(row.id)} collapsing to {@code "[object Object]"}), and forces every consuming
 * application to write and register its own custom serializer/deserializer.
 *
 * <p>{@code quarkus-morphium} 6.3.1 removes that requirement entirely. As soon as the extension
 * detects a compatible JSON layer on the classpath, it registers the matching module automatically
 * -- no configuration, no code in this application:
 * <ul>
 *   <li>{@code quarkus-rest-jackson} on the classpath (the case in <em>this</em> showcase, see
 *       {@code pom.xml}) &rarr; the extension's {@code MorphiumIdJacksonModule}
 *       ({@code de.caluga.morphium.quarkus.json.MorphiumIdJacksonModule}, an
 *       {@code ObjectMapperCustomizer}) registers a Jackson {@code SimpleModule} that serializes
 *       {@code MorphiumId} as {@code value.toString()} and deserializes it back via
 *       {@code new MorphiumId(hexString)}.</li>
 *   <li>{@code quarkus-jsonb} on the classpath &rarr; the extension's {@code MorphiumIdJsonbModule}
 *       ({@code de.caluga.morphium.quarkus.json.MorphiumIdJsonbModule}, a
 *       {@code JsonbConfigCustomizer}) registers {@code MorphiumIdJsonbAdapter}
 *       ({@code de.caluga.morphium.quarkus.json.MorphiumIdJsonbAdapter}) on the JSON-B config,
 *       achieving the exact same hex-string wire format via JSON-B's {@code JsonbAdapter} SPI
 *       instead of Jackson's module SPI.</li>
 * </ul>
 * Either way, the result on the wire is identical: a plain 24-character hex string, both ways.
 *
 * <h2>What this resource demonstrates</h2>
 * <p>This class is intentionally kept separate from the feature-oriented resources
 * ({@code ProductResource}, {@code BlogResource}, ...), which mostly render Qute/HTML pages. It
 * reuses the existing {@link Product} entity (unmodified -- see
 * {@code catalog/entity/Product.java}, whose {@code @Id private MorphiumId id;} field is exactly
 * what triggers the extension's automatic handling) and exposes it purely as
 * {@code application/json}, so the raw wire format is visible with a plain {@code curl}:
 * <pre>{@code
 * curl -s localhost:8080/api/morphium-id-json/products | jq '.[0].id'
 * # "68a1c2f3e4d5a6b7c8d9e0f1"   <-- a plain string, not {"pid":..,"counter":..,...}
 * }</pre>
 * <ul>
 *   <li>{@link #listProducts()} -- serialization: every {@code Product.id} in the response is a
 *       flat hex string.</li>
 *   <li>{@link #getProduct(String)} -- serialization of a single entity fetched by that same hex
 *       string via the path parameter.</li>
 *   <li>{@link #echoProduct(Product)} -- deserialization: a JSON request body containing
 *       {@code "id": "<24-hex-chars>"} is parsed straight back into a real {@code Product} with a
 *       real {@code MorphiumId} instance in its {@code id} field, then echoed back unchanged. No
 *       {@code @JsonSerialize}/{@code @JsonDeserialize} annotation appears anywhere on
 *       {@code Product} or in this class -- the extension's Jackson module (registered
 *       automatically, see above) is solely responsible.</li>
 * </ul>
 *
 * <p><b>No custom serializer in this application.</b> A repository-wide search
 * ({@code grep -rn "JsonSerializer\|@JsonSerialize\|MorphiumId"} across
 * {@code src/main/java}) confirms there is no hand-written {@code MorphiumId} (de)serializer
 * anywhere in this showcase -- every hit is either an {@code import} of {@code MorphiumId} itself
 * or an unrelated {@code @Id MorphiumId id} field declaration. The plain-hex-string wire format
 * seen through this resource is provided entirely by the extension.
 */
@Path("/api/morphium-id-json")
public class MorphiumIdJsonResource {

    @Inject
    ProductService productService;

    /**
     * Returns all products as JSON. Ensures sample data exists first.
     *
     * <p>Every element's {@code id} field is the plain 24-character hex string produced by the
     * extension's automatic {@code MorphiumId} Jackson module -- not a nested
     * {@code {pid, counter, machineId, bytes, time}} object.</p>
     */
    @GET
    @Path("/products")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Product> listProducts() {
        productService.seedData();
        return productService.findAll();
    }

    /**
     * Returns a single product as JSON, looked up by its {@code MorphiumId} hex string.
     *
     * @param id the 24-character hex string identifying the product (the same string that
     *           appeared in the {@code id} field of {@link #listProducts()})
     */
    @GET
    @Path("/products/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getProduct(@PathParam("id") String id) {
        Product product = productService.findById(id);
        if (product == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("No product found for MorphiumId hex string: " + id)
                    .build();
        }
        return Response.ok(product).build();
    }

    /**
     * Accepts a {@link Product} JSON body -- including its {@code id} field as a plain hex
     * string -- and echoes it straight back <em>without persisting it</em>.
     *
     * <p>This round-trips the {@code id} field through Jackson deserialization
     * ({@code hex string -> MorphiumId}) and back through serialization
     * ({@code MorphiumId -> hex string}), proving both directions work with zero custom code in
     * this application. Try it:</p>
     * <pre>{@code
     * curl -s -X POST localhost:8080/api/morphium-id-json/products/echo \
     *   -H 'Content-Type: application/json' \
     *   -d '{"id":"507f1f77bcf86cd799439011","name":"Echo Test","price":1.0,"stock":1}' | jq .
     * }</pre>
     *
     * @param product the deserialized product; its {@code id} field is already a real
     *                 {@code MorphiumId} instance by the time this method body runs
     */
    @POST
    @Path("/products/echo")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Product echoProduct(Product product) {
        return product;
    }
}
