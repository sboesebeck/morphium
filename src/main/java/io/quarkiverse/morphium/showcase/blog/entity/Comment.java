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
package io.quarkiverse.morphium.showcase.blog.entity;

import de.caluga.morphium.annotations.Embedded;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;

/**
 * An embedded document representing a comment on a {@link BlogPost}.
 *
 * <p>Like {@link io.quarkiverse.morphium.showcase.catalog.entity.Category}, this class uses
 * {@code @Embedded} to indicate that it is stored as a sub-document within its parent entity
 * (BlogPost), not in a separate collection.</p>
 *
 * <h3>Why @Embedded for comments?</h3>
 * <p>Comments are a natural fit for embedding because:</p>
 * <ul>
 *   <li>They belong to exactly one blog post and are never shared.</li>
 *   <li>They are always loaded together with the post (no need for separate queries).</li>
 *   <li>Embedding avoids the overhead of JOINs/references -- a single document read gets
 *       the post with all its comments.</li>
 * </ul>
 *
 * <p>In MongoDB, a BlogPost document with comments looks like:</p>
 * <pre>{@code
 * {
 *   "_id": ObjectId("..."),
 *   "title": "My Post",
 *   "comments": [
 *     { "author": "alice", "text": "Great post!", "created_at": ISODate("...") },
 *     { "author": "bob", "text": "Thanks for sharing", "created_at": ISODate("...") }
 *   ]
 * }
 * }</pre>
 *
 * <p><strong>Caveat:</strong> MongoDB has a 16MB document size limit. If a post could accumulate
 * thousands of comments, a separate collection with {@code @Reference} might be more appropriate.</p>
 */
// @Embedded: Stored as a sub-document inside the parent BlogPost, not in a separate collection.
// No @Id field is needed for embedded documents.
@Embedded
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class Comment {

    /** The name/username of the commenter. Stored as a plain string (not a @Reference to Author). */
    private String author;

    /** The comment text. */
    private String text;

    /**
     * The timestamp when this comment was created.
     *
     * <p>Note: {@code @CreationTime} is NOT used here because it only works on top-level
     * {@code @Entity} classes, not on {@code @Embedded} objects. For embedded documents,
     * you set the timestamp manually in application code (see
     * {@link io.quarkiverse.morphium.showcase.blog.BlogService#addComment}).</p>
     */
    private LocalDateTime createdAt;
}