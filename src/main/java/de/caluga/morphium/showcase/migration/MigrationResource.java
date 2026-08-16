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
package de.caluga.morphium.showcase.migration;

import de.caluga.morphium.showcase.common.DocLink;
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
import java.util.List;

@Path("/migration")
public class MigrationResource {

    @Inject
    Template migration;

    @Inject
    @Location("tags/learn-migration.html")
    Template learnMigration;

    @Inject
    @Location("tags/demo-migration.html")
    Template demoMigration;

    @Inject
    MigrationService migrationService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "@MorphiumChangeUnit, @Execution, @RollbackExecution"),
            new DocLink("/docs/configuration-reference", "Configuration Reference", "quarkus.morphium.migration.*")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        migrationService.seedIfEmpty();
        return migration.data("active", "migration")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnMigration.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        migrationService.seedIfEmpty();
        return demoData(null, null, null, null);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    private TemplateInstance demoData(String success, String error,
            String lastOperation, String lastMongoCommand) {
        return demoMigration.data("notes", migrationService.findAllNotes())
                .data("noteCount", migrationService.noteCount())
                .data("unmigratedCount", migrationService.unmigratedCount())
                .data("changelogEntries", migrationService.findChangelogEntries())
                .data("lockHeld", migrationService.isLockHeld())
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    @POST
    @Path("/run")
    @Produces(MediaType.TEXT_HTML)
    public Response runMigrations(@Context HttpHeaders headers) {
        migrationService.runMigrationsNow();
        if (isHtmx(headers)) return Response.ok(demoData("Migration run triggered.", null,
                "new MorphiumMigrationRunner(morphium, config).execute([...])",
                "db.migration_notes.updateMany({archived: null}, {$set: {archived: false}})")).build();
        return Response.seeOther(URI.create("/migration")).build();
    }

    @POST
    @Path("/reset")
    @Produces(MediaType.TEXT_HTML)
    public Response reset(@Context HttpHeaders headers) {
        migrationService.reset();
        migrationService.seedIfEmpty();
        if (isHtmx(headers)) return Response.ok(demoData("Reset — notes re-seeded without the archived field, changelog cleared.", null,
                "morphium.dropCollection(MigrationNote.class)", "db.migration_notes.drop()")).build();
        return Response.seeOther(URI.create("/migration")).build();
    }
}
