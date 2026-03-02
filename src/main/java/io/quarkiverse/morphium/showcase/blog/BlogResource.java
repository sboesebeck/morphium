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
package io.quarkiverse.morphium.showcase.blog;

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
import java.util.Arrays;
import java.util.List;

@Path("/blog")
public class BlogResource {

    @Inject
    Template blog;

    @Inject
    @Location("tags/learn-blog.html")
    Template learnBlog;

    @Inject
    @Location("tags/demo-blog.html")
    Template demoBlog;

    @Inject
    BlogService blogService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "@Version, @Reference, @Embedded, Lifecycle"),
            new DocLink("/docs/howtos/optimistic-locking", "Optimistic Locking", "@Version, VersionMismatchException"),
            new DocLink("/docs/api-reference", "API Reference", "store(), createQueryFor(), @Reference eager/lazy")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        blogService.seedData();
        return blog.data("active", "blog")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnBlog.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        blogService.seedData();
        return demoData(null, null);
    }

    private TemplateInstance demoData(String success, String error) {
        return demoBlog.data("posts", blogService.findAllPosts())
                .data("authors", blogService.findAllAuthors())
                .data("count", blogService.findAllPosts().size())
                .data("successMessage", success)
                .data("errorMessage", error);
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }

    @POST
    @Path("/authors")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createAuthor(
            @FormParam("name") String name,
            @FormParam("email") String email,
            @FormParam("bio") String bio,
            @Context HttpHeaders headers) {
        blogService.createAuthor(name, name, email, bio);
        if (isHtmx(headers)) return Response.ok(demoData("Author '" + name + "' created.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response createPost(
            @FormParam("title") String title,
            @FormParam("content") String content,
            @FormParam("authorId") String authorId,
            @FormParam("tags") String tagsStr,
            @Context HttpHeaders headers) {
        List<String> tags = tagsStr != null && !tagsStr.isBlank()
                ? Arrays.asList(tagsStr.split(",\\s*"))
                : List.of();
        blogService.createPost(title, content, authorId, tags);
        if (isHtmx(headers)) return Response.ok(demoData("Post '" + title + "' created.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts/{id}/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response updatePost(
            @PathParam("id") String id,
            @FormParam("title") String title,
            @FormParam("content") String content,
            @Context HttpHeaders headers) {
        try {
            blogService.updatePost(id, title, content);
            if (isHtmx(headers)) return Response.ok(demoData("Post updated (version incremented).", null)).build();
        } catch (Exception e) {
            if (isHtmx(headers)) return Response.ok(demoData(null, "Update failed: " + e.getMessage())).build();
        }
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts/{id}/comment")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response addComment(
            @PathParam("id") String id,
            @FormParam("author") String author,
            @FormParam("text") String text,
            @Context HttpHeaders headers) {
        blogService.addComment(id, author, text);
        if (isHtmx(headers)) return Response.ok(demoData("Comment added.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts/{id}/publish")
    @Produces(MediaType.TEXT_HTML)
    public Response publishPost(@PathParam("id") String id, @Context HttpHeaders headers) {
        blogService.publishPost(id);
        if (isHtmx(headers)) return Response.ok(demoData("Post published.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }

    @DELETE
    @Path("/posts/{id}")
    @Produces(MediaType.TEXT_HTML)
    public Response deletePost(@PathParam("id") String id, @Context HttpHeaders headers) {
        blogService.deletePost(id);
        if (isHtmx(headers)) return Response.ok(demoData("Post deleted.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        blogService.resetData();
        if (isHtmx(headers)) return Response.ok(demoData("Sample data re-seeded.", null)).build();
        return Response.seeOther(URI.create("/blog")).build();
    }
}
