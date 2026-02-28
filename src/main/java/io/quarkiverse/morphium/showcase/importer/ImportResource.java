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
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

@Path("/importer")
public class ImportResource {

    @Inject
    Template importer;

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
                .data("records", importService.findAll(50))
                .data("count", importService.count())
                .data("bulkImportDuration", null)
                .data("bulkImportCount", null)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/bulk-import")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance bulkImport(@FormParam("count") int count) {
        long durationMs = importService.bulkImport(count);
        return importer.data("active", "importer")
                .data("records", importService.findAll(50))
                .data("count", importService.count())
                .data("bulkImportDuration", durationMs)
                .data("bulkImportCount", count)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response addTag(@PathParam("id") String id, @FormParam("tag") String tag) {
        importService.addTag(id, tag);
        return Response.seeOther(URI.create("/importer")).build();
    }

    @DELETE
    @Path("/records/{id}/tag")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response removeTag(@PathParam("id") String id, @FormParam("tag") String tag) {
        importService.removeTag(id, tag);
        return Response.seeOther(URI.create("/importer")).build();
    }

    @POST
    @Path("/records/{id}/process")
    public Response markProcessed(@PathParam("id") String id) {
        importService.markProcessed(id);
        return Response.seeOther(URI.create("/importer")).build();
    }

    @POST
    @Path("/seed")
    public Response seed() {
        importService.deleteAll();
        importService.seedData();
        return Response.seeOther(URI.create("/importer")).build();
    }

    @DELETE
    @Path("/records")
    public Response deleteAll() {
        importService.deleteAll();
        return Response.seeOther(URI.create("/importer")).build();
    }
}