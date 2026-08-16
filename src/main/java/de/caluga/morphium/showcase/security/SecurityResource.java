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
package de.caluga.morphium.showcase.security;

import de.caluga.morphium.showcase.common.DocLink;
import de.caluga.morphium.showcase.security.entity.SecureNote;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/security")
public class SecurityResource {

    @Inject
    Template security;

    @Inject
    @Location("tags/learn-security.html")
    Template learnSecurity;

    @Inject
    @Location("tags/demo-security.html")
    Template demoSecurity;

    @Inject
    EncryptionService encryptionService;

    @Inject
    JCacheService jCacheService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/security-guide", "Security Guide", "@Encrypted, EncryptionKeyProvider"),
            new DocLink("/docs/developer-guide", "Developer Guide", "@Cache, MorphiumCache")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        jCacheService.seedIfEmpty();
        return security.data("active", "security")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnSecurity.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        jCacheService.seedIfEmpty();
        return demoData(null, null, null, null, null);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    private TemplateInstance demoData(String success, String error,
            String lastOperation, String lastMongoCommand, Map<String, Object> rawDoc) {
        List<SecureNote> notes = encryptionService.findAll();
        SecureNote latest = notes.isEmpty() ? null : notes.get(0);
        Map<String, Object> raw = rawDoc;
        if (raw == null && latest != null) {
            raw = encryptionService.readRawDocument(latest);
        }
        String rawSecretDescription = raw == null ? null
                : encryptionService.describeRawValue(raw.get("secret_content"));

        return demoSecurity.data("notes", notes)
                .data("noteCount", encryptionService.count())
                .data("latestNote", latest)
                .data("rawSecretDescription", rawSecretDescription)
                .data("jcacheReports", jCacheService.findAll())
                .data("jcacheReportCount", jCacheService.count())
                .data("jcacheSizes", jCacheService.cacheSizes())
                .data("jcacheTiming", null)
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    @POST
    @Path("/notes")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createNote(
            @FormParam("title") String title,
            @FormParam("secretContent") String secretContent,
            @FormParam("category") String category,
            @Context HttpHeaders headers) {
        SecureNote note = encryptionService.createNote(title, secretContent, category);
        Map<String, Object> raw = encryptionService.readRawDocument(note);
        if (isHtmx(headers)) return Response.ok(demoData("Note created — secretContent was encrypted before it hit MongoDB.", null,
                "morphium.store(note)", "db.secure_notes.insertOne({..., secretContent: <byte[]>})", raw)).build();
        return Response.seeOther(URI.create("/security")).build();
    }

    @POST
    @Path("/reset")
    @Produces(MediaType.TEXT_HTML)
    public Response reset(@Context HttpHeaders headers) {
        encryptionService.deleteAll();
        jCacheService.reset();
        jCacheService.seedIfEmpty();
        if (isHtmx(headers)) return Response.ok(demoData("Reset — secure notes cleared, JCache reports re-seeded.", null,
                "morphium.dropCollection(...)", "db.secure_notes.drop()", null)).build();
        return Response.seeOther(URI.create("/security")).build();
    }

    @POST
    @Path("/jcache/compare")
    @Produces(MediaType.TEXT_HTML)
    public Response jcacheCompare(@Context HttpHeaders headers) {
        long[] timings = jCacheService.timedHitMissComparison();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("missNanos", timings[0]);
        result.put("hitNanos", timings[1]);
        result.put("missMs", timings[0] / 1_000_000.0);
        result.put("hitMs", timings[1] / 1_000_000.0);

        List<SecureNote> notes = encryptionService.findAll();
        SecureNote latest = notes.isEmpty() ? null : notes.get(0);
        Map<String, Object> raw = latest == null ? null : encryptionService.readRawDocument(latest);
        String rawSecretDescription = raw == null ? null
                : encryptionService.describeRawValue(raw.get("secret_content"));

        TemplateInstance ti = demoSecurity.data("notes", notes)
                .data("noteCount", encryptionService.count())
                .data("latestNote", latest)
                .data("rawSecretDescription", rawSecretDescription)
                .data("jcacheReports", jCacheService.findAll())
                .data("jcacheReportCount", jCacheService.count())
                .data("jcacheSizes", jCacheService.cacheSizes())
                .data("jcacheTiming", result)
                .data("successMessage", "Hit/miss comparison run — see timing below.")
                .data("errorMessage", null)
                .data("lastOperation", "createQueryFor(JCacheReport.class).asList() -- twice")
                .data("lastMongoCommand", "db.jcache_reports.find({})");

        if (isHtmx(headers)) return Response.ok(ti).build();
        return Response.seeOther(URI.create("/security")).build();
    }
}
