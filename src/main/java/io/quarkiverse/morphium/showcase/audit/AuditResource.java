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

/**
 * JAX-RS resource demonstrating Morphium queries on a <strong>capped collection</strong>,
 * including sorting, limiting, and counting documents.
 *
 * <p>This resource provides three endpoints:</p>
 * <ul>
 *   <li>{@code GET /audit} -- Displays the most recent 50 audit entries, demonstrating
 *       Morphium's {@code sort()}, {@code limit()}, and {@code asList()} query methods.</li>
 *   <li>{@code POST /audit/log} -- Creates a single audit entry via form submission.</li>
 *   <li>{@code POST /audit/seed} -- Populates the audit log with 20 sample entries.</li>
 * </ul>
 *
 * <h3>Morphium Query API Demonstrated</h3>
 * <p>The {@code page()} method showcases a typical Morphium query pattern:</p>
 * <pre>{@code
 * morphium.createQueryFor(AuditEntry.class)  // 1. Create a typed query
 *     .sort(Map.of("timestamp", -1))          // 2. Add sort criteria
 *     .limit(50)                               // 3. Limit result count
 *     .asList();                               // 4. Execute and get results as List
 * }</pre>
 *
 * @see AuditEntry the capped-collection entity
 * @see AuditListener the service that stores audit entries
 */
@Path("/audit")
public class AuditResource {

    /** Qute template for the audit log HTML page. */
    @Inject
    Template audit;

    /**
     * Direct Morphium instance, used here to show that you can perform queries directly
     * in the resource layer without necessarily going through a service class. In larger
     * applications, you would typically encapsulate Morphium calls in a repository or
     * service class.
     */
    @Inject
    Morphium morphium;

    /** The audit listener service used to create new audit entries. */
    @Inject
    AuditListener auditListener;

    /** Documentation links displayed on the audit page. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "@Capped Collections, @Lifecycle"),
            new DocLink("/docs/api-reference", "API Reference", "store(), sort(), limit(), countAll()")
    );

    /**
     * Renders the audit log page, showing the 50 most recent entries and total count.
     *
     * <p>This method demonstrates Morphium's fluent query API:</p>
     * <ul>
     *   <li>{@code createQueryFor(Class)} -- Creates a type-safe query for the given entity.</li>
     *   <li>{@code sort(Map)} -- Adds sort criteria. The map key is the field name and the
     *       value is the sort direction: {@code -1} for descending, {@code 1} for ascending.
     *       Here we use {@code AuditEntry.Fields.timestamp} (from Lombok's {@code @FieldNameConstants})
     *       for type-safe field name references.</li>
     *   <li>{@code limit(int)} -- Restricts the number of returned documents (maps to
     *       MongoDB's cursor {@code limit}).</li>
     *   <li>{@code asList()} -- Executes the query and returns results as a {@code List}.
     *       Alternative terminal operations include {@code get()} (single result),
     *       {@code countAll()}, and {@code distinct()}.</li>
     * </ul>
     *
     * @return a Qute template instance populated with audit entries and count
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        // Query: find all AuditEntry documents, sorted by timestamp descending (newest first),
        // limited to 50 results. This is the most common Morphium query pattern.
        // Note: AuditEntry.Fields.timestamp is a Lombok-generated constant containing the
        // string "timestamp" -- using it avoids typos in field name references.
        List<AuditEntry> entries = morphium.createQueryFor(AuditEntry.class)
                .sort(Map.of(AuditEntry.Fields.timestamp, -1))
                .limit(50)
                .asList();

        // countAll() -- Returns the total number of documents in the collection (no filter).
        // This is a separate query because limit() above only affects the returned documents,
        // not the count.
        long count = morphium.createQueryFor(AuditEntry.class).countAll();

        return audit.data("active", "audit")
                .data("entries", entries)
                .data("count", count)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Creates a single audit log entry from form data.
     *
     * <p>Delegates to {@link AuditListener#log} which calls {@code morphium.store()}.
     * The user is hardcoded to "web-user" for this demo endpoint. After storing,
     * redirects back to the audit page (POST-Redirect-GET pattern).</p>
     *
     * @param entityType the type of entity being audited
     * @param entityId   the entity's identifier
     * @param action     the action performed (CREATE, UPDATE, DELETE)
     * @param details    description of the change
     * @return a 303 See Other redirect to the audit page
     */
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

    /**
     * Seeds the audit log with 20 randomized sample entries for demonstration.
     *
     * <p>Each entry is stored individually via {@link AuditListener#log}, which calls
     * {@code morphium.store()} for each one. In a production scenario with many entries,
     * you would use {@code morphium.storeList()} for better performance (see
     * {@link io.quarkiverse.morphium.showcase.analytics.AnalyticsService#seedData()} for
     * a bulk-insert example).</p>
     *
     * <p>Because the underlying collection is capped (10,000 max entries), you can call
     * this endpoint repeatedly without worrying about unbounded growth -- MongoDB will
     * automatically evict the oldest entries when the cap is reached.</p>
     *
     * @return a 303 See Other redirect to the audit page
     */
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

            // Each call to log() triggers a separate morphium.store() insert into the
            // capped "audit_log" collection. The @CreationTime annotation on AuditEntry
            // ensures each entry gets an automatic timestamp.
            auditListener.log(entityType, entityId, action, details, user);
        }
        return Response.seeOther(URI.create("/audit")).build();
    }
}