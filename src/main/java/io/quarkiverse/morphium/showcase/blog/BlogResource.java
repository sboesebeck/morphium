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
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
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
    BlogService blogService;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/howtos/optimistic-locking", "Optimistic Locking", "@Version, VersionMismatchException"),
            new DocLink("/docs/developer-guide", "Developer Guide", "Embedded vs Reference, Lifecycle"),
            new DocLink("/docs/api-reference", "API Reference", "store(), Query, @Reference")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        blogService.seedData();
        return blog.data("active", "blog")
                .data("posts", blogService.findAllPosts())
                .data("authors", blogService.findAllAuthors())
                .data("updateResult", null)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/posts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createPost(
            @FormParam("title") String title,
            @FormParam("content") String content,
            @FormParam("authorId") String authorId,
            @FormParam("tags") String tagsStr) {
        List<String> tags = tagsStr != null && !tagsStr.isBlank()
                ? Arrays.asList(tagsStr.split(",\\s*"))
                : List.of();
        blogService.createPost(title, content, authorId, tags);
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts/{id}/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance updatePost(
            @PathParam("id") String id,
            @FormParam("title") String title,
            @FormParam("content") String content) {
        var result = blogService.updatePost(id, title, content);
        return blog.data("active", "blog")
                .data("posts", blogService.findAllPosts())
                .data("authors", blogService.findAllAuthors())
                .data("updateResult", result)
                .data("docLinks", DOC_LINKS);
    }

    @POST
    @Path("/posts/{id}/comment")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response addComment(
            @PathParam("id") String id,
            @FormParam("author") String author,
            @FormParam("text") String text) {
        blogService.addComment(id, author, text);
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/posts/{id}/publish")
    public Response publishPost(@PathParam("id") String id) {
        blogService.publishPost(id);
        return Response.seeOther(URI.create("/blog")).build();
    }

    @DELETE
    @Path("/posts/{id}")
    public Response deletePost(@PathParam("id") String id) {
        blogService.deletePost(id);
        return Response.seeOther(URI.create("/blog")).build();
    }

    @POST
    @Path("/authors")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createAuthor(
            @FormParam("username") String username,
            @FormParam("displayName") String displayName,
            @FormParam("email") String email,
            @FormParam("bio") String bio) {
        blogService.createAuthor(username, displayName, email, bio);
        return Response.seeOther(URI.create("/blog")).build();
    }
}