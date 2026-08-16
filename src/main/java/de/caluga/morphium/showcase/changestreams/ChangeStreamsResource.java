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
package de.caluga.morphium.showcase.changestreams;

import de.caluga.morphium.showcase.changestreams.entity.LiveNote;
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
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;

import java.net.URI;
import java.util.List;

/**
 * JAX-RS resource for the <strong>Change Streams</strong> showcase feature.
 *
 * <p>Serves the Learn/Demo tabbed page (same pattern as every other showcase feature) plus a
 * Server-Sent Events endpoint ({@code GET /changestreams/events}) that streams
 * {@link ChangeEventView} objects to the browser in realtime as documents in the
 * {@link LiveNote} collection are inserted, updated, or deleted -- via
 * {@link LiveNoteChangeStreamService}, which owns the actual Morphium
 * {@code ChangeStreamMonitor}.</p>
 *
 * <p>The demo form actions ({@code /notes}, {@code /notes/{id}}, ...) intentionally do
 * <strong>not</strong> push anything to the SSE stream themselves -- the whole point is that the
 * live feed reacts purely to what happened in MongoDB, discovered independently via the change
 * stream, exactly as it would for writes made by a completely different process.</p>
 */
@Path("/changestreams")
public class ChangeStreamsResource {

    @Inject
    Template changestreams;

    @Inject
    @Location("tags/learn-changestreams.html")
    Template learnChangeStreams;

    @Inject
    @Location("tags/demo-changestreams.html")
    Template demoChangeStreams;

    @Inject
    LiveNoteService liveNoteService;

    @Inject
    LiveNoteChangeStreamService changeStreamService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "ChangeStreamMonitor, watch(), resume tokens"),
            new DocLink("/docs/api-reference", "API Reference", "store(), set(), delete(), createQueryFor()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        return changestreams.data("active", "changestreams")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnChangeStreams.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        return demoData(null, null, null, null);
    }

    private TemplateInstance demoData(String success, String error,
            String lastOperation, String lastMongoCommand) {
        List<LiveNote> notes = liveNoteService.findAll();
        return demoChangeStreams.data("notes", notes)
                .data("count", liveNoteService.count())
                .data("watching", changeStreamService.isWatching())
                .data("subscriberCount", changeStreamService.subscriberCount())
                .data("successMessage", success)
                .data("errorMessage", error)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/notes")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createNote(
            @FormParam("title") String title,
            @FormParam("content") String content,
            @FormParam("author") String author,
            @Context HttpHeaders headers) {
        liveNoteService.create(title, content, author);
        if (isHtmx(headers)) return Response.ok(demoData("Note created — watch the live feed update.", null,
                "morphium.store(liveNote)", "db.live_notes.insertOne({...})")).build();
        return Response.seeOther(URI.create("/changestreams")).build();
    }

    @POST
    @Path("/notes/{id}/edit")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response editNote(
            @PathParam("id") String id,
            @FormParam("content") String content,
            @Context HttpHeaders headers) {
        liveNoteService.updateContent(id, content);
        if (isHtmx(headers)) return Response.ok(demoData("Note updated — an 'update' change event was emitted.", null,
                "query.set(\"content\", newContent)", "db.live_notes.updateOne({_id: ...}, {$set: {content: ..., updated_at: ...}})")).build();
        return Response.seeOther(URI.create("/changestreams")).build();
    }

    @POST
    @Path("/notes/{id}/delete")
    @Produces(MediaType.TEXT_HTML)
    public Response deleteNote(@PathParam("id") String id, @Context HttpHeaders headers) {
        liveNoteService.delete(id);
        if (isHtmx(headers)) return Response.ok(demoData("Note deleted — a 'delete' change event was emitted.", null,
                "query.delete()", "db.live_notes.deleteOne({_id: ...})")).build();
        return Response.seeOther(URI.create("/changestreams")).build();
    }

    @POST
    @Path("/reset")
    @Produces(MediaType.TEXT_HTML)
    public Response reset(@Context HttpHeaders headers) {
        liveNoteService.deleteAll();
        changeStreamService.clearHistory();
        if (isHtmx(headers)) return Response.ok(demoData("All notes removed and event history cleared.", null,
                "query.delete()  (matching all documents)", "db.live_notes.deleteMany({})")).build();
        return Response.seeOther(URI.create("/changestreams")).build();
    }

    /**
     * Server-Sent Events endpoint streaming live {@link ChangeEventView} notifications.
     *
     * <p>Each connecting browser tab gets its own {@link SseEventSink}. On connect, we first
     * replay {@link LiveNoteChangeStreamService#recentHistory()} so a tab opened after some
     * activity already has already-happened events to show, then register a subscriber that
     * forwards every new change stream event for the lifetime of the connection.</p>
     *
     * <p>This mirrors the messaging feature's realtime pattern, but the <em>source</em> of
     * truth here is MongoDB's own change stream rather than a Morphium queue/topic -- i.e. this
     * endpoint would work identically even if {@code LiveNote} documents were written by a
     * completely different application connected to the same MongoDB deployment.</p>
     */
    @GET
    @Path("/events")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamEvents(@Context SseEventSink sink, @Context Sse sse) {
        // Replay recent history first so the client has immediate context.
        for (ChangeEventView evt : changeStreamService.recentHistory()) {
            sink.send(sse.newEventBuilder()
                    .name("change")
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .data(ChangeEventView.class, evt)
                    .build());
        }

        // SseEventSink has no close-listener in the JAX-RS API, so the subscriber unsubscribes
        // itself the moment it notices the sink has been closed (browser tab closed/navigated
        // away). We need the Runnable reference inside the lambda, hence the array indirection.
        Runnable[] unsubscribeHolder = new Runnable[1];
        unsubscribeHolder[0] = changeStreamService.subscribe(evt -> {
            if (sink.isClosed()) {
                unsubscribeHolder[0].run();
                return;
            }
            sink.send(sse.newEventBuilder()
                    .name("change")
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .data(ChangeEventView.class, evt)
                    .build())
                    .exceptionally(t -> {
                        unsubscribeHolder[0].run();
                        return null;
                    });
        });
    }
}
