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

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

/**
 * Morphium entity representing a blog author, stored in its own collection.
 *
 * <p>This entity is designed to be referenced from {@link BlogPost} via Morphium's {@code @Reference}
 * annotation. Unlike {@code @Embedded} objects (which are stored inline), an {@code @Entity} with
 * {@code @Reference} lives in its own collection and is linked by its {@code _id}.</p>
 *
 * <p>Key annotations demonstrated:</p>
 * <ul>
 *   <li><strong>{@code @Entity(collectionName = "authors")}</strong> -- Stores Author documents in
 *       the "authors" collection. Authors exist independently and can be referenced by multiple
 *       blog posts.</li>
 *   <li><strong>{@code @Index(options = {"unique:1"})}</strong> -- Ensures that no two authors can
 *       have the same username. MongoDB enforces this constraint at the database level, even under
 *       concurrent inserts.</li>
 * </ul>
 *
 * <h3>@Entity vs @Embedded -- design decision</h3>
 * <p>Authors are {@code @Entity} (not {@code @Embedded}) because:</p>
 * <ol>
 *   <li>Multiple blog posts can reference the same author without duplicating data.</li>
 *   <li>Authors can be queried and managed independently of blog posts.</li>
 *   <li>Updating an author's profile automatically reflects in all referencing posts.</li>
 * </ol>
 * <p>Compare this with {@link Comment}, which is {@code @Embedded} because comments belong to
 * exactly one blog post and are always accessed together with it.</p>
 */
// @Entity: This class gets its own MongoDB collection ("authors").
// It requires an @Id field and can be referenced from other entities via @Reference.
@Entity(collectionName = "authors")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class Author {

    /**
     * The MongoDB document {@code _id}. When this Author is referenced from a BlogPost via
     * {@code @Reference}, Morphium stores this id in the referencing document and resolves
     * it automatically on load.
     */
    @Id
    private MorphiumId id;

    /**
     * A unique username for the author.
     *
     * <p>{@code @Index(options = {"unique:1"})} creates a unique index, so MongoDB rejects any
     * attempt to insert a duplicate username. This is the recommended way to enforce uniqueness
     * in MongoDB -- application-level checks are insufficient under concurrent access.</p>
     */
    @Index(options = {"unique:1"})
    private String username;

    /** The author's display name (e.g., "Alice Smith"). Not indexed since it is not queried directly. */
    private String displayName;

    /** The author's email address. */
    private String email;

    /** A short biography. */
    private String bio;
}