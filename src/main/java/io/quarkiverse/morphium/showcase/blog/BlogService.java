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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.VersionMismatchException;
import io.quarkiverse.morphium.showcase.blog.entity.Author;
import io.quarkiverse.morphium.showcase.blog.entity.BlogPost;
import io.quarkiverse.morphium.showcase.blog.entity.Comment;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Service layer demonstrating Morphium's reference handling, lifecycle callbacks, optimistic locking
 * with version conflict detection, and embedded document management.
 *
 * <p>Key Morphium features demonstrated:</p>
 * <ul>
 *   <li><strong>{@code @Reference} resolution</strong> -- When loading a BlogPost, Morphium
 *       automatically resolves the {@code author} field by querying the "authors" collection.
 *       For {@code reviewer} (with {@code lazyLoading = true}), the query is deferred until
 *       the reviewer object is actually accessed.</li>
 *   <li><strong>{@code @Version} and VersionMismatchException</strong> -- The {@code updatePost()}
 *       method shows how to catch and gracefully handle optimistic locking conflicts.</li>
 *   <li><strong>Embedded document manipulation</strong> -- Adding comments to a post demonstrates
 *       working with embedded ({@code @Embedded}) sub-documents stored in a list.</li>
 *   <li><strong>Lifecycle callbacks</strong> -- BlogPost's {@code @PreStore} and {@code @PostLoad}
 *       methods are invoked automatically by Morphium during store/load operations.</li>
 * </ul>
 */
@ApplicationScoped
public class BlogService {

    @Inject
    Morphium morphium;

    /**
     * Retrieves all blog posts, sorted by creation time descending (newest first).
     *
     * <p>When Morphium loads each BlogPost, it automatically resolves the {@code @Reference author}
     * field by performing an additional query against the "authors" collection. The
     * {@code @Reference(lazyLoading = true) reviewer} field is NOT loaded until accessed.</p>
     *
     * @return all blog posts, newest first
     */
    public List<BlogPost> findAllPosts() {
        // sort() with -1 means descending order. Morphium translates this to MongoDB's
        // { createdAt: -1 } sort specification.
        return morphium.createQueryFor(BlogPost.class)
                .sort(Map.of(BlogPost.Fields.createdAt, -1))
                .asList();
    }

    /**
     * Finds a single blog post by its MongoDB ObjectId.
     *
     * @param id the string representation of the MorphiumId
     * @return the matching blog post with its author resolved, or {@code null} if not found
     */
    public BlogPost findPostById(String id) {
        // MorphiumId can be constructed from a hex string representation of a MongoDB ObjectId.
        // get() returns a single result or null (unlike asList() which returns a list).
        return morphium.createQueryFor(BlogPost.class)
                .f(BlogPost.Fields.id).eq(new MorphiumId(id))
                .get();
    }

    /**
     * Creates a new blog post, optionally linking it to an author via {@code @Reference}.
     *
     * <p>When storing a BlogPost with a non-null {@code author} field, Morphium stores only the
     * author's {@code _id} as a DBRef in the blog post document -- the Author object is NOT
     * duplicated. The author must already exist in the "authors" collection.</p>
     *
     * @param title    the post title
     * @param content  the post content
     * @param authorId the author's MorphiumId as a string (can be null)
     * @param tags     a list of tags
     * @return the newly created blog post
     */
    public BlogPost createPost(String title, String content, String authorId, List<String> tags) {
        Author author = null;
        if (authorId != null && !authorId.isBlank()) {
            // Load the Author entity so Morphium can store a @Reference to it in the BlogPost.
            // The author must be a persisted entity (with a valid _id) for the reference to work.
            author = morphium.createQueryFor(Author.class)
                    .f(Author.Fields.id).eq(new MorphiumId(authorId))
                    .get();
        }

        BlogPost post = BlogPost.builder()
                .title(title)
                .content(content)
                .author(author)
                .tags(tags)
                .published(false)
                .metadata(Map.of("source", "web"))
                .build();
        // store() will trigger the @PreStore lifecycle callback (onPreStore) before writing,
        // and Morphium will auto-populate @Id, @Version, @CreationTime, @LastChange.
        // The author field is stored as a DBRef (reference), not embedded.
        morphium.store(post);
        return post;
    }

    /**
     * Result record for update operations, carrying success status, a message, and the updated post.
     */
    public record UpdateResult(boolean success, String message, BlogPost post) {}

    /**
     * Updates a blog post's title and content, demonstrating optimistic locking conflict handling.
     *
     * <p>Because BlogPost has a {@code @Version} field, Morphium performs a conditional update:
     * it only writes the changes if the version in the database matches the version read by this
     * method. If another process modified the post between the read and the write, Morphium throws
     * {@link VersionMismatchException} instead of silently overwriting the other process's changes.</p>
     *
     * <p>This is the standard pattern for handling version conflicts:</p>
     * <ol>
     *   <li>Load the entity (version = N)</li>
     *   <li>Modify fields in Java</li>
     *   <li>Call {@code store()} -- Morphium sends: UPDATE WHERE _id = ... AND version = N</li>
     *   <li>If successful: version becomes N+1</li>
     *   <li>If VersionMismatchException: inform the user to reload and retry</li>
     * </ol>
     *
     * @param id      the post id
     * @param title   the new title
     * @param content the new content
     * @return an {@link UpdateResult} indicating success or a version conflict
     */
    public UpdateResult updatePost(String id, String title, String content) {
        BlogPost post = findPostById(id);
        if (post == null) return new UpdateResult(false, "Post not found", null);

        post.setTitle(title);
        post.setContent(content);
        try {
            // store() will check that the @Version in the DB matches the version on this object.
            // If they match, the update proceeds and version is incremented.
            // If they don't match (concurrent modification), VersionMismatchException is thrown.
            morphium.store(post);
            return new UpdateResult(true, "Post updated (version: " + post.getVersion() + ")", post);
        } catch (VersionMismatchException e) {
            // This exception means another client/thread modified the post since we read it.
            // The correct response is to notify the user and let them reload + retry.
            return new UpdateResult(false, "Version conflict! Someone else edited this post. Please reload.", post);
        }
    }

    /**
     * Adds a comment to a blog post, demonstrating embedded document manipulation.
     *
     * <p>Since {@link Comment} is {@code @Embedded}, it is stored directly inside the BlogPost
     * document's "comments" array. Adding a comment means modifying the parent BlogPost and
     * re-storing it -- there is no separate "comments" collection.</p>
     *
     * <p>Note: The comment's {@code createdAt} is set manually here because {@code @CreationTime}
     * only works on top-level {@code @Entity} classes, not on {@code @Embedded} objects.</p>
     *
     * @param postId the blog post's id
     * @param author the comment author's name
     * @param text   the comment text
     */
    public void addComment(String postId, String author, String text) {
        BlogPost post = findPostById(postId);
        if (post != null) {
            // Manually set createdAt because @CreationTime does not apply to @Embedded objects.
            Comment comment = Comment.builder()
                    .author(author)
                    .text(text)
                    .createdAt(LocalDateTime.now())
                    .build();
            // Add the comment to the in-memory list, then store the entire BlogPost.
            // Morphium replaces the full document in MongoDB, including the updated comments array.
            post.getComments().add(comment);
            morphium.store(post);
        }
    }

    /**
     * Publishes a blog post by setting its {@code published} flag to {@code true}.
     *
     * @param id the blog post id
     */
    public void publishPost(String id) {
        BlogPost post = findPostById(id);
        if (post != null) {
            post.setPublished(true);
            // store() replaces the entire document. The @Version is incremented,
            // and @LastChange is updated automatically by Morphium.
            morphium.store(post);
        }
    }

    /**
     * Deletes a blog post by its id.
     *
     * <p>Note: Deleting a BlogPost does NOT delete the referenced Author, because {@code @Reference}
     * does not cascade deletes. The Author continues to exist in the "authors" collection.</p>
     *
     * @param id the blog post id
     */
    public void deletePost(String id) {
        BlogPost post = findPostById(id);
        if (post != null) {
            // delete() removes the document matching this entity's @Id from MongoDB.
            morphium.delete(post);
        }
    }

    // --- Author management ---

    /**
     * Retrieves all authors from the "authors" collection.
     *
     * @return a list of all authors
     */
    public List<Author> findAllAuthors() {
        return morphium.createQueryFor(Author.class).asList();
    }

    /**
     * Creates a new author and persists it.
     *
     * <p>Authors must be stored before they can be used as {@code @Reference} targets in BlogPost.
     * Morphium needs a valid {@code _id} on the Author to create the DBRef.</p>
     *
     * @param username    the unique username
     * @param displayName the display name
     * @param email       the email address
     * @param bio         a short biography
     * @return the newly created author with its auto-generated id
     */
    public Author createAuthor(String username, String displayName, String email, String bio) {
        Author author = Author.builder()
                .username(username)
                .displayName(displayName)
                .email(email)
                .bio(bio)
                .build();
        // store() inserts the author into the "authors" collection and generates the @Id.
        morphium.store(author);
        return author;
    }

    /**
     * Seeds sample blog data (authors, posts with comments) if the database is empty.
     *
     * <p>Demonstrates:</p>
     * <ul>
     *   <li>Creating referenced entities (Authors) before the referencing entity (BlogPost)</li>
     *   <li>Storing a BlogPost with both {@code @Reference author} and
     *       {@code @Reference(lazyLoading = true) reviewer}</li>
     *   <li>Adding embedded comments to a persisted post</li>
     * </ul>
     */
    public void resetData() {
        morphium.dropCollection(BlogPost.class);
        morphium.dropCollection(Author.class);
        seedData();
    }

    public void seedData() {
        if (morphium.createQueryFor(Author.class).countAll() > 0) return;

        // Create authors first -- they must exist in the DB before being referenced.
        Author alice = createAuthor("alice", "Alice Smith", "alice@example.com", "Tech blogger");
        Author bob = createAuthor("bob", "Bob Johnson", "bob@example.com", "Java enthusiast");

        // Post 1: Has an eager @Reference to alice as author.
        BlogPost post1 = BlogPost.builder()
                .title("Getting Started with Morphium")
                .content("Morphium is a powerful MongoDB ORM for Java...")
                .author(alice)
                .tags(List.of("morphium", "mongodb", "java"))
                .published(true)
                .metadata(Map.of("difficulty", "beginner"))
                .build();
        morphium.store(post1);

        // Add embedded comments to the stored post.
        addComment(post1.getId().toString(), "bob", "Great introduction!");
        addComment(post1.getId().toString(), "charlie", "Very helpful, thanks!");

        // Post 2: Has both author (eager) and reviewer (lazy) references.
        BlogPost post2 = BlogPost.builder()
                .title("Advanced Morphium Queries")
                .content("Let's explore advanced query patterns with Morphium...")
                .author(bob)
                .reviewer(alice)
                .tags(List.of("morphium", "queries", "advanced"))
                .published(true)
                .metadata(Map.of("difficulty", "advanced", "series", "morphium-deep-dive"))
                .build();
        morphium.store(post2);

        // Post 3: An unpublished draft (published = false).
        BlogPost post3 = BlogPost.builder()
                .title("Draft: Morphium Caching Strategies")
                .content("This post is still in progress...")
                .author(alice)
                .tags(List.of("morphium", "caching", "draft"))
                .published(false)
                .build();
        morphium.store(post3);
    }
}