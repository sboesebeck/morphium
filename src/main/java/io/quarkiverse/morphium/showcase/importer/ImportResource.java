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
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

@Path("/importer")
public class ImportResource {

    @Inject
    Template importer;

    @Inject
    @Location("tags/learn-importer.html")
    Template learnImporter;

    @Inject
    @Location("tags/demo-importer.html")
    Template demoImporter;

    @Inject
    ImportService importService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/api-reference", "API Reference", "storeList(), push(), pull(), set(), unset()"),
            new DocLink("/docs/developer-guide", "Developer Guide", "Batch Operations, @WriteBuffer"),
            new DocLink("/docs/performance-scalability-guide", "Performance Guide", "Bulk Writes, SequenceGenerator")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        importService.seedData();
        return importer.data("active", "importer")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnImporter.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        importService.seedData();
        return demoData(null, null, null, null, null, null);
    }

    private TemplateInstance demoData(String success, String error, Long duration, Integer importCount,
            String lastOperation, String lastMongoCommand) {
        return demoImporter.data("records", importService.findAll(50))
                .data("count", importService.count())
                .data("bulkImportDuration", duration)
                .data("bulkImportCount", importCount)
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/bulk-import")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    @RunOnVirtualThread
    public Response bulkImport(@FormParam("count") int count, @Context HttpHeaders headers) {
        long durationMs = importService.bulkImport(count);
        if (isHtmx(headers)) {
            return Response.ok(demoData("Imported " + count + " records in " + durationMs + "ms.", null, durationMs, count,
                    "morphium.storeList(records)", "db.import_records.insertMany([...])")).build();
        }
        return Response.ok(importer.data("active", "importer").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response addTag(@PathParam("id") String id, @FormParam("tag") String tag, @Context HttpHeaders headers) {
        importService.addTag(id, tag);
        if (isHtmx(headers)) return Response.ok(demoData("Tag added.", null, null, null,
                "morphium.push(record, \"tags\", tag)", "db.import_records.updateOne({_id: ...}, {$push: {tags: ...}})")).build();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @DELETE
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response removeTag(@PathParam("id") String id, @FormParam("tag") String tag, @Context HttpHeaders headers) {
        importService.removeTag(id, tag);
        if (isHtmx(headers)) return Response.ok(demoData("Tag removed.", null, null, null,
                "morphium.pull(record, \"tags\", tag)", "db.import_records.updateOne({_id: ...}, {$pull: {tags: ...}})")).build();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @POST
    @Path("/records/{id}/process")
    public Response markProcessed(@PathParam("id") String id, @Context HttpHeaders headers) {
        importService.markProcessed(id);
        if (isHtmx(headers)) return Response.ok(demoData("Record marked as processed.", null, null, null,
                "morphium.set(record, \"processed\", true)", "db.import_records.updateOne({_id: ...}, {$set: {processed: true}})")).build();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @POST
    @Path("/seed")
    public Response seed(@Context HttpHeaders headers) {
        importService.deleteAll();
        importService.seedData();
        if (isHtmx(headers)) return Response.ok(demoData("Sample data re-seeded.", null, null, null,
                "morphium.storeList(records)", "db.import_records.insertMany([...])")).build();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @DELETE
    @Path("/records")
    public Response deleteAll(@Context HttpHeaders headers) {
        importService.deleteAll();
        if (isHtmx(headers)) return Response.ok(demoData("All records deleted.", null, null, null,
                "morphium.dropCollection(ImportRecord.class)", "db.import_records.drop()")).build();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @POST
    @Path("/records/delete")
    public Response deleteAllViaPost(@Context HttpHeaders headers) {
        return deleteAll(headers);
    }
}
