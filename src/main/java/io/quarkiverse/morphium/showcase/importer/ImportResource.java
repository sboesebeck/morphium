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
package io.quarkiverse.morphium.showcase.importer;

import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

/**
 * JAX-RS resource exposing the Morphium data importer showcase as an HTML UI.
 *
 * <p>This resource demonstrates how Morphium's bulk write, array manipulation, and field-level
 * update capabilities can be used in a typical data import/processing pipeline. Endpoints
 * delegate to {@link ImportService} which performs the actual Morphium operations.</p>
 *
 * <p><b>Morphium features exposed through this resource:</b></p>
 * <ul>
 *   <li>Bulk import with {@code storeList()} and {@code SequenceGenerator} (via {@code /bulk-import})</li>
 *   <li>Array manipulation with {@code push()} and {@code pull()} (via {@code /records/{id}/tag})</li>
 *   <li>Field-level updates with {@code query.set()} (via {@code /records/{id}/process})</li>
 *   <li>Collection management with {@code dropCollection()} (via {@code /seed} and {@code /records})</li>
 * </ul>
 */
@Path("/importer")
public class ImportResource {

    @Inject
    Template importer;

    @Inject
    ImportService importService;

    /** Links to Morphium documentation relevant to the import features shown on this page. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/api-reference", "API Reference", "storeList(), push(), pull(), set(), unset()"),
            new DocLink("/docs/developer-guide", "Developer Guide", "Batch Operations, @WriteBuffer"),
            new DocLink("/docs/performance-scalability-guide", "Performance Guide", "Bulk Writes, SequenceGenerator")
    );

    /**
     * Renders the importer showcase page with the most recent 50 import records.
     *
     * <p>Seeds sample data on first access to ensure the demo has content to display.</p>
     *
     * @return a Qute template instance populated with import records and metadata
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        importService.seedData();
        return importer.data("active", "importer")
                .data("records", importService.findAll(50))
                .data("count", importService.count())
                .data("bulkImportDuration", null)
                .data("bulkImportCount", null)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Triggers a bulk import of the specified number of records.
     *
     * <p>Demonstrates Morphium's {@code storeList()} for high-performance batch inserts
     * and {@code SequenceGenerator} for generating unique sequential IDs. The response
     * includes the elapsed time so users can observe the performance characteristics
     * of bulk writes vs. individual inserts.</p>
     *
     * @param count the number of records to generate and import in bulk
     * @return the importer page showing the imported records and timing information
     */
    @POST
    @Path("/bulk-import")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    @RunOnVirtualThread
    public TemplateInstance bulkImport(@FormParam("count") int count) {
        long durationMs = importService.bulkImport(count);
        return importer.data("active", "importer")
                .data("records", importService.findAll(50))
                .data("count", importService.count())
                .data("bulkImportDuration", durationMs)
                .data("bulkImportCount", count)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Adds a tag to an import record using Morphium's {@code push()} operation.
     *
     * <p>Demonstrates MongoDB's {@code $push} operator through Morphium -- appending
     * a value to an array field without loading the document.</p>
     *
     * @param id  the MorphiumId of the import record
     * @param tag the tag string to add to the record's tags array
     * @return a redirect response to the importer page
     */
    @POST
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response addTag(@PathParam("id") String id, @FormParam("tag") String tag) {
        importService.addTag(id, tag);
        return Response.seeOther(URI.create("/importer")).build();
    }

    /**
     * Removes a tag from an import record using Morphium's {@code pull()} operation.
     *
     * <p>Demonstrates MongoDB's {@code $pull} operator through Morphium -- removing
     * all occurrences of a value from an array field without loading the document.</p>
     *
     * @param id  the MorphiumId of the import record
     * @param tag the tag string to remove from the record's tags array
     * @return a redirect response to the importer page
     */
    @DELETE
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response removeTag(@PathParam("id") String id, @FormParam("tag") String tag) {
        importService.removeTag(id, tag);
        return Response.seeOther(URI.create("/importer")).build();
    }

    /**
     * Marks an import record as processed using Morphium's {@code query.set()} operation.
     *
     * <p>Demonstrates targeted field-level updates -- only the "status" field is modified
     * via MongoDB's {@code $set} operator, leaving all other fields untouched.</p>
     *
     * @param id the MorphiumId of the import record to mark as processed
     * @return a redirect response to the importer page
     */
    @POST
    @Path("/records/{id}/process")
    public Response markProcessed(@PathParam("id") String id) {
        importService.markProcessed(id);
        return Response.seeOther(URI.create("/importer")).build();
    }

    /**
     * Resets the demo data by dropping the collection and re-seeding with sample records.
     *
     * @return a redirect response to the importer page
     */
    @POST
    @Path("/seed")
    public Response seed() {
        importService.deleteAll();
        importService.seedData();
        return Response.seeOther(URI.create("/importer")).build();
    }

    /**
     * Deletes all import records by dropping the entire MongoDB collection.
     *
     * <p>Uses {@code morphium.dropCollection()} which removes the collection, all documents,
     * and all indexes in a single operation.</p>
     *
     * @return a redirect response to the importer page
     */
    @DELETE
    @Path("/records")
    public Response deleteAll() {
        importService.deleteAll();
        return Response.seeOther(URI.create("/importer")).build();
    }

    /**
     * POST-based delete-all for HTML forms (browsers only support GET/POST).
     * Delegates to {@link #deleteAll()}.
     */
    @POST
    @Path("/records/delete")
    public Response deleteAllViaPost() {
        return deleteAll();
    }
}
