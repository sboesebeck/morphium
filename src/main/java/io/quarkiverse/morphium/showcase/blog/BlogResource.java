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

/**
 * JAX-RS resource serving the blog demo pages.
 *
 * <p>This resource demonstrates how Morphium handles complex document relationships through the
 * {@link BlogService}. Key Morphium features exposed through this UI:</p>
 * <ul>
 *   <li><strong>References</strong> -- Blog posts reference authors via {@code @Reference} (eager)
 *       and {@code @Reference(lazyLoading = true)}</li>
 *   <li><strong>Embedded documents</strong> -- Comments are stored inline within blog post documents</li>
 *   <li><strong>Optimistic locking</strong> -- The update endpoint shows version conflict detection
 *       via {@code VersionMismatchException}</li>
 *   <li><strong>Lifecycle callbacks</strong> -- BlogPost's {@code @PreStore} and {@code @PostLoad}
 *       methods are triggered automatically during persistence operations</li>
 * </ul>
 */
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

    /**
     * Renders the main blog page with all posts and authors.
     * Seeds sample data (authors, posts, comments) on first access.
     *
     * @return the rendered blog HTML page
     */
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

    /**
     * Creates a new blog post with an optional author reference and tags.
     * Demonstrates Morphium's {@code @Reference} handling -- the author is stored as a DBRef,
     * not embedded.
     *
     * @param title    the post title
     * @param content  the post content
     * @param authorId the author's MorphiumId string (optional)
     * @param tagsStr  comma-separated tags
     * @return a redirect to the blog page
     */
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

    /**
     * Updates a blog post's title and content. This endpoint demonstrates Morphium's optimistic
     * locking: if another user edited the post concurrently, the update returns a version conflict
     * message instead of silently overwriting.
     *
     * @param id      the post id
     * @param title   the new title
     * @param content the new content
     * @return the blog page with an update result (success or version conflict)
     */
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

    /**
     * Adds an embedded comment to a blog post. The comment is stored as a sub-document
     * within the post's "comments" array -- no separate collection is involved.
     *
     * @param id     the post id
     * @param author the comment author's name
     * @param text   the comment text
     * @return a redirect to the blog page
     */
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

    /**
     * Publishes a blog post by setting its published flag to true.
     *
     * @param id the post id
     * @return a redirect to the blog page
     */
    @POST
    @Path("/posts/{id}/publish")
    public Response publishPost(@PathParam("id") String id) {
        blogService.publishPost(id);
        return Response.seeOther(URI.create("/blog")).build();
    }

    /**
     * Deletes a blog post. Note: the referenced Author is NOT deleted (no cascade).
     *
     * @param id the post id
     * @return a redirect to the blog page
     */
    @DELETE
    @Path("/posts/{id}")
    public Response deletePost(@PathParam("id") String id) {
        blogService.deletePost(id);
        return Response.seeOther(URI.create("/blog")).build();
    }

    /**
     * Creates a new author. Authors must be created before they can be assigned to blog posts
     * via {@code @Reference}.
     *
     * @param username    the unique username
     * @param displayName the display name
     * @param email       the email address
     * @param bio         a short biography
     * @return a redirect to the blog page
     */
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