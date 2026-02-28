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
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

/**
 * JAX-RS resource exposing the Morphium messaging showcase as an HTML UI.
 *
 * <p>This resource demonstrates how a Quarkus REST endpoint can integrate with Morphium
 * through a service layer. It provides endpoints for viewing, sending, and managing chat
 * messages stored in MongoDB via Morphium ORM. The HTML rendering is handled by Quarkus
 * Qute templates.</p>
 *
 * <p><b>Morphium features demonstrated through this resource:</b></p>
 * <ul>
 *   <li>Querying with filters and sorting (via {@link MessagingService#findByTopic(String)})</li>
 *   <li>Document creation with auto-generated IDs and timestamps (via {@link MessagingService#send})</li>
 *   <li>Targeted field-level updates using {@code $set} (via {@link MessagingService#markAsRead(String)})</li>
 *   <li>Collection-level operations like drop and count (via {@link MessagingService#deleteAll()})</li>
 * </ul>
 */
@Path("/messaging")
public class MessagingResource {

    @Inject
    Template messaging;

    @Inject
    MessagingService messagingService;

    /** Links to Morphium documentation relevant to the messaging features shown on this page. */
    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/messaging", "Messaging System", "Built-in Queue, Topics, Listeners"),
            new DocLink("/docs/howtos/messaging-implementations", "Messaging Implementations", "sendMessage(), addListenerForTopic()"),
            new DocLink("/docs/api-reference", "API Reference", "query.set(), countAll(), sort()")
    );

    /**
     * Renders the messaging showcase page, optionally filtered by topic.
     *
     * <p>On first access, seeds sample data to ensure the demo has content to display.
     * If a {@code topic} query parameter is provided, only messages for that topic are shown;
     * otherwise, all messages are listed.</p>
     *
     * @param topic optional topic filter passed as a query parameter (e.g., {@code ?topic=general})
     * @return a Qute template instance populated with messages, topics, and metadata
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page(@QueryParam("topic") String topic) {
        messagingService.seedData();
        var messages = (topic != null && !topic.isBlank())
                ? messagingService.findByTopic(topic)
                : messagingService.findAll();
        return messaging.data("active", "messaging")
                .data("messages", messages)
                .data("topics", messagingService.getTopics())
                .data("count", messagingService.count())
                .data("selectedTopic", topic)
                .data("docLinks", DOC_LINKS);
    }

    /**
     * Sends a new chat message and redirects back to the messaging page.
     *
     * <p>Delegates to {@link MessagingService#send} which uses {@code morphium.store()}
     * to persist the new message. The {@code @CreationTime} field is automatically populated.</p>
     *
     * @param sender    the sender's username
     * @param recipient the recipient's username
     * @param topic     the message topic/channel
     * @param text      the message body text
     * @return a redirect response to the messaging page
     */
    @POST
    @Path("/send")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response sendMessage(
            @FormParam("sender") String sender,
            @FormParam("recipient") String recipient,
            @FormParam("topic") String topic,
            @FormParam("text") String text) {
        messagingService.send(sender, recipient, topic, text);
        return Response.seeOther(URI.create("/messaging")).build();
    }

    /**
     * Marks a message as read using a targeted MongoDB {@code $set} update.
     *
     * <p>This demonstrates Morphium's ability to update a single field without loading
     * the full document. See {@link MessagingService#markAsRead(String)} for details.</p>
     *
     * @param id the MorphiumId of the message to mark as read (passed as a path parameter)
     * @return a redirect response to the messaging page
     */
    @POST
    @Path("/read/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response markAsRead(@PathParam("id") String id) {
        messagingService.markAsRead(id);
        return Response.seeOther(URI.create("/messaging")).build();
    }

    /**
     * Resets the demo data by dropping the collection and re-seeding.
     *
     * <p>Uses {@code morphium.dropCollection()} to remove all documents and indexes,
     * then re-inserts the sample data. This is a common pattern for demo/test resets.</p>
     *
     * @return a redirect response to the messaging page
     */
    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed() {
        messagingService.deleteAll();
        messagingService.seedData();
        return Response.seeOther(URI.create("/messaging")).build();
    }
}
