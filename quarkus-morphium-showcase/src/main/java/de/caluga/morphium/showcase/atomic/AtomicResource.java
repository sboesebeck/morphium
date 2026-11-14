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
package de.caluga.morphium.showcase.atomic;

import de.caluga.morphium.showcase.atomic.entity.JobState;
import de.caluga.morphium.showcase.common.DocLink;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

/**
 * "Atomic Updates" (7.0): the update builder, leases, derived update methods and what a rejected write looks like. The page shows the outcome of each operation; the
 * state is the tickets table and the lease.
 */
@Path("/atomic")
public class AtomicResource {

    @Inject
    Template atomic;

    @Inject
    @Location("tags/learn-atomic.html")
    Template learnAtomic;

    @Inject
    @Location("tags/demo-atomic.html")
    Template demoAtomic;

    @Inject
    AtomicService service;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/api-reference", "API Reference", "Atomic update builder, LeaseManager"),
            new DocLink("/docs/howtos/update-pitfalls", "Update Pitfalls", "raw maps, upserts, rejected writes"),
            new DocLink("/docs/howtos/conditional-writes", "Conditional Writes", "leases, claims, run-once markers"),
            new DocLink("/docs/jakarta-data", "Jakarta Data", "@AtomicUpdate, updateBy..."),
            new DocLink("/docs/howtos/migration-v6-to-v7", "Migrating from 6 to 7", "what changed for writes")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        service.seed();
        return atomic.data("active", "atomic").data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnAtomic.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        service.seed();
        return demo(null, null, null);
    }

    @POST
    @Path("/tickets/{id}/claim")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance claim(@PathParam("id") String id, @FormParam("worker") String worker) {
        return demo(service.claim(id, worker),
                "query.f(\"id\").eq(id).f(\"state\").eq(NEW).modify().set(...).inc(\"attempts\", 1).tryFindOneAndUpdate(AFTER)",
                "db.atomic_tickets.findOneAndUpdate({_id: id, state: 'NEW'}, {$set: {state: 'RUNNING', owner: ..., started_at: ISODate(...), changed_at: ...}, $inc: {attempts: 1}})");
    }

    @POST
    @Path("/tickets/{id}/code")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance changeCode(@PathParam("id") String id, @FormParam("code") String code) {
        return demo(service.changeCode(id, code),
                "query.f(\"id\").eq(id).set(\"code\", code)",
                "db.atomic_tickets.updateOne({_id: id}, {$set: {code: ..., changed_at: ...}})  // unique index on code");
    }

    @POST
    @Path("/assign")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance assign(@FormParam("state") JobState state, @FormParam("owner") String owner, @FormParam("ticket") String ticket) {
        boolean one = ticket != null && !ticket.isBlank();
        String message = one
                ? service.assignOne(ticket.trim(), state, owner)
                : service.assignAll(state, owner) + " ticket(s) in state " + state + " belong to " + owner + " now.";
        return demo(message,
                one ? "repository.updateByIdAndState(id, state, owner)" : "repository.updateByState(state, owner)",
                one ? "db.atomic_tickets.findOneAndUpdate({_id: id, state: state}, {$set: {owner: owner, changed_at: ...}})"
                    : "db.atomic_tickets.updateMany({state: state}, {$set: {owner: owner, changed_at: ...}})");
    }

    @POST
    @Path("/lease/acquire")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance acquire(@FormParam("worker") String worker) {
        return demo(service.acquireLease(worker),
                "LeaseManager.of(morphium, WorkLease.class, \"ownerName\", \"expiresAt\").tryAcquire(key, worker, ttl)",
                "db.atomic_leases.findOneAndUpdate({_id: key, $or: [{expires_at: {$lt: now}}, {owner_name: worker}]}, {$set: {owner_name: ..., expires_at: ...}}, {upsert: true})");
    }

    @POST
    @Path("/lease/release")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance release(@FormParam("worker") String worker) {
        return demo(service.releaseLease(worker), "leaseManager.release(key, worker)",
                "db.atomic_leases.updateOne({_id: key, owner_name: worker}, {$set: {expires_at: <past>}})");
    }

    @POST
    @Path("/reset")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance reset() {
        service.reset();
        return demo("Sample data restored.", null, null);
    }

    private TemplateInstance demo(String message, String operation, String mongoCommand) {
        return demoAtomic.data("tickets", service.findAll())
                .data("lease", service.currentLease())
                .data("states", JobState.values())
                .data("message", message)
                .data("lastOperation", operation)
                .data("lastMongoCommand", mongoCommand);
    }
}
