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
package io.quarkiverse.morphium.showcase.audit;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.audit.entity.AuditEntry;
import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;
import java.util.Map;

@Path("/audit")
public class AuditResource {

    @Inject
    Template audit;

    @Inject
    Morphium morphium;

    @Inject
    AuditListener auditListener;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "@Capped Collections, @Lifecycle"),
            new DocLink("/docs/api-reference", "API Reference", "store(), sort(), limit(), countAll()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        List<AuditEntry> entries = morphium.createQueryFor(AuditEntry.class)
                .sort(Map.of(AuditEntry.Fields.timestamp, -1))
                .limit(50)
                .asList();
        long count = morphium.createQueryFor(AuditEntry.class).countAll();
        return audit.data("active", "audit")
                .data("entries", entries)
                .data("count", count)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/log")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response logEntry(
            @FormParam("entityType") String entityType,
            @FormParam("entityId") String entityId,
            @FormParam("action") String action,
            @FormParam("details") String details) {
        auditListener.log(entityType, entityId, action, details, "web-user");
        return Response.seeOther(URI.create("/audit")).build();
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        String[] entityTypes = {"Product", "Account", "BlogPost", "Store", "ChatMessage"};
        String[] actions = {"CREATE", "UPDATE", "DELETE"};
        String[] users = {"alice", "bob", "charlie", "system"};

        java.util.Random rand = new java.util.Random(42);
        for (int i = 0; i < 20; i++) {
            String entityType = entityTypes[rand.nextInt(entityTypes.length)];
            String action = actions[rand.nextInt(actions.length)];
            String user = users[rand.nextInt(users.length)];
            String entityId = entityType.toLowerCase() + "-" + (rand.nextInt(100) + 1);
            String details = action + " operation on " + entityType + " #" + entityId + " by " + user;
            auditListener.log(entityType, entityId, action, details, user);
        }
        return Response.seeOther(URI.create("/audit")).build();
    }
}