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

@ApplicationScoped
public class BlogService {

    @Inject
    Morphium morphium;

    public List<BlogPost> findAllPosts() {
        return morphium.createQueryFor(BlogPost.class)
                .sort(Map.of(BlogPost.Fields.createdAt, -1))
                .asList();
    }

    public BlogPost findPostById(String id) {
        return morphium.createQueryFor(BlogPost.class)
                .f(BlogPost.Fields.id).eq(new MorphiumId(id))
                .get();
    }

    public BlogPost createPost(String title, String content, String authorId, List<String> tags) {
        Author author = null;
        if (authorId != null && !authorId.isBlank()) {
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
        morphium.store(post);
        return post;
    }

    public record UpdateResult(boolean success, String message, BlogPost post) {}

    public UpdateResult updatePost(String id, String title, String content) {
        BlogPost post = findPostById(id);
        if (post == null) return new UpdateResult(false, "Post not found", null);

        post.setTitle(title);
        post.setContent(content);
        try {
            morphium.store(post);
            return new UpdateResult(true, "Post updated (version: " + post.getVersion() + ")", post);
        } catch (VersionMismatchException e) {
            return new UpdateResult(false, "Version conflict! Someone else edited this post. Please reload.", post);
        }
    }

    public void addComment(String postId, String author, String text) {
        BlogPost post = findPostById(postId);
        if (post != null) {
            Comment comment = Comment.builder()
                    .author(author)
                    .text(text)
                    .createdAt(LocalDateTime.now())
                    .build();
            post.getComments().add(comment);
            morphium.store(post);
        }
    }

    public void publishPost(String id) {
        BlogPost post = findPostById(id);
        if (post != null) {
            post.setPublished(true);
            morphium.store(post);
        }
    }

    public void deletePost(String id) {
        BlogPost post = findPostById(id);
        if (post != null) {
            morphium.delete(post);
        }
    }

    // Author management
    public List<Author> findAllAuthors() {
        return morphium.createQueryFor(Author.class).asList();
    }

    public Author createAuthor(String username, String displayName, String email, String bio) {
        Author author = Author.builder()
                .username(username)
                .displayName(displayName)
                .email(email)
                .bio(bio)
                .build();
        morphium.store(author);
        return author;
    }

    public void seedData() {
        if (morphium.createQueryFor(Author.class).countAll() > 0) return;

        Author alice = createAuthor("alice", "Alice Smith", "alice@example.com", "Tech blogger");
        Author bob = createAuthor("bob", "Bob Johnson", "bob@example.com", "Java enthusiast");

        BlogPost post1 = BlogPost.builder()
                .title("Getting Started with Morphium")
                .content("Morphium is a powerful MongoDB ORM for Java...")
                .author(alice)
                .tags(List.of("morphium", "mongodb", "java"))
                .published(true)
                .metadata(Map.of("difficulty", "beginner"))
                .build();
        morphium.store(post1);

        addComment(post1.getId().toString(), "bob", "Great introduction!");
        addComment(post1.getId().toString(), "charlie", "Very helpful, thanks!");

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