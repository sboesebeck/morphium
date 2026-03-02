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
package io.quarkiverse.morphium.showcase.messaging;

import io.quarkiverse.morphium.showcase.common.DocLink;
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

@Path("/messaging")
public class MessagingResource {

    @Inject
    Template messaging;

    @Inject
    @Location("tags/learn-messaging.html")
    Template learnMessaging;

    @Inject
    @Location("tags/demo-messaging.html")
    Template demoMessaging;

    @Inject
    MessagingService messagingService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/messaging", "Messaging System", "Built-in Queue, Topics, Listeners"),
            new DocLink("/docs/howtos/messaging-implementations", "Messaging Implementations", "sendMessage(), addListenerForTopic()"),
            new DocLink("/docs/api-reference", "API Reference", "query.set(), countAll(), sort()")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page(@QueryParam("topic") String topic) {
        messagingService.seedData();
        return messaging.data("active", "messaging")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnMessaging.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab(@QueryParam("topic") String topic) {
        messagingService.seedData();
        var messages = (topic != null && !topic.isBlank())
                ? messagingService.findByTopic(topic)
                : messagingService.findAll();
        return demoMessaging.data("messages", messages)
                .data("topics", messagingService.getTopics())
                .data("count", messagingService.count())
                .data("selectedTopic", topic)
                .data("successMessage", null)
                .data("errorMessage", null);
    }

    private TemplateInstance demoData(String success, String error) {
        return demoMessaging.data("messages", messagingService.findAll())
                .data("topics", messagingService.getTopics())
                .data("count", messagingService.count())
                .data("selectedTopic", null)
                .data("successMessage", success)
                .data("errorMessage", error);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/send")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response sendMessage(
            @FormParam("sender") String sender,
            @FormParam("recipient") String recipient,
            @FormParam("topic") String topic,
            @FormParam("text") String text,
            @Context HttpHeaders headers) {
        messagingService.send(sender, recipient, topic, text);
        if (isHtmx(headers)) return Response.ok(demoData("Message sent.", null)).build();
        return Response.seeOther(URI.create("/messaging")).build();
    }

    @POST
    @Path("/read/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response markAsRead(@PathParam("id") String id, @Context HttpHeaders headers) {
        messagingService.markAsRead(id);
        if (isHtmx(headers)) return Response.ok(demoData("Message marked as read.", null)).build();
        return Response.seeOther(URI.create("/messaging")).build();
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        messagingService.deleteAll();
        messagingService.seedData();
        if (isHtmx(headers)) return Response.ok(demoData("Sample data re-seeded.", null)).build();
        return Response.seeOther(URI.create("/messaging")).build();
    }
}
