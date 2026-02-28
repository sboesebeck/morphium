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
package io.quarkiverse.morphium.showcase.query.entity;

import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.IgnoreNullFromDB;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PostLoad;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Morphium entity representing an employee, showcasing advanced annotation features.
 *
 * <p>This entity demonstrates the richest set of Morphium annotations in the showcase project,
 * covering indexing, field aliases, lifecycle callbacks, transient fields, and null handling.
 * It serves as a comprehensive reference for Morphium's entity mapping capabilities.</p>
 *
 * <h3>Annotations demonstrated:</h3>
 *
 * <h4>{@code @Entity(collectionName = "employees")}</h4>
 * <p>Maps this class to the {@code employees} MongoDB collection. Required on every
 * Morphium-managed entity class.</p>
 *
 * <h4>{@code @Index({"lastName, firstName"})} -- Compound Index (class-level)</h4>
 * <p>Declares a compound index on the {@code lastName} and {@code firstName} fields.
 * Compound indexes are essential for queries that filter or sort by multiple fields --
 * MongoDB can use a single compound index to satisfy queries on any prefix of the indexed
 * fields. Here, queries like "find by lastName" or "find by lastName AND firstName" will
 * benefit from this index. The index is automatically created by Morphium when the collection
 * is first accessed (if auto-index creation is enabled).</p>
 *
 * <h4>{@code @Lifecycle}</h4>
 * <p>Enables lifecycle callback processing for this entity class. Without this class-level
 * annotation, Morphium will NOT scan for or invoke any lifecycle method annotations
 * ({@code @PostLoad}, {@code @PreStore}, {@code @PostStore}, etc.) on this class.
 * This is a performance optimization -- Morphium only checks for lifecycle methods on
 * classes explicitly marked with {@code @Lifecycle}.</p>
 *
 * <h4>{@code @Aliases}</h4>
 * <p>Declares alternative field names that Morphium should recognize when reading documents
 * from MongoDB. This is invaluable for schema evolution -- if a field was previously stored
 * under a different name (e.g., "mail" or "e_mail"), Morphium will still map those legacy
 * field names to the current Java field.</p>
 *
 * <h4>{@code @Index} (field-level)</h4>
 * <p>Creates a single-field index on the annotated field. Applied to {@code department} here
 * to speed up department-based queries and distinct operations.</p>
 *
 * <h4>{@code @Transient}</h4>
 * <p>Marks a field as NOT persisted to MongoDB. The field exists only in the Java object
 * and is never written to or read from the database. Useful for computed/derived values.</p>
 *
 * <h4>{@code @IgnoreNullFromDB}</h4>
 * <p>If the field is {@code null} in the MongoDB document (or missing entirely), Morphium
 * will keep the Java field's current value instead of overwriting it with {@code null}.
 * This is useful for fields with default initializers or for defensive null handling.</p>
 *
 * <h4>{@code @PostLoad}</h4>
 * <p>A lifecycle callback method invoked automatically by Morphium after a document is loaded
 * from MongoDB and deserialized into this object. Used here to compute the {@code fullName}
 * transient field from {@code firstName} and {@code lastName}.</p>
 *
 * @see de.caluga.morphium.annotations.Entity
 * @see de.caluga.morphium.annotations.Index
 * @see de.caluga.morphium.annotations.Aliases
 * @see de.caluga.morphium.annotations.Transient
 * @see de.caluga.morphium.annotations.IgnoreNullFromDB
 * @see de.caluga.morphium.annotations.lifecycle.Lifecycle
 * @see de.caluga.morphium.annotations.lifecycle.PostLoad
 */
@Entity(collectionName = "employees")
@Index({"lastName, firstName"})
@Lifecycle
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class Employee {

    /**
     * Unique identifier mapped to MongoDB's {@code _id} field.
     * Auto-generated by Morphium on first store if left {@code null}.
     */
    @Id
    private MorphiumId id;

    /** Employee's first name. Part of the compound index {lastName, firstName}. */
    private String firstName;

    /** Employee's last name. Part of the compound index {lastName, firstName}. */
    private String lastName;

    /**
     * Employee's email address.
     *
     * <p>The {@code @Aliases({"mail", "e_mail"})} annotation tells Morphium to also look for
     * fields named {@code "mail"} or {@code "e_mail"} in the MongoDB document when reading.
     * This is a schema evolution feature: if older documents stored the email under a different
     * field name, Morphium will still correctly map the value to this Java field.</p>
     *
     * <p><b>Important:</b> Aliases only affect reading. When writing, Morphium always uses
     * the Java field name (or the {@code @Property} name if specified). Over time, as documents
     * are re-saved, the field names will naturally migrate to the current naming convention.</p>
     */
    @Aliases({"mail", "e_mail"})
    private String email;

    /**
     * Department the employee belongs to (e.g., "Engineering", "Marketing", "Sales", "HR").
     *
     * <p>The field-level {@code @Index} annotation creates a single-field index on
     * {@code department}. This accelerates queries that filter by department, such as
     * {@code f("department").eq("Engineering")} and the {@code distinct("department")}
     * operation. Without this index, MongoDB would perform a full collection scan for
     * every department-based query.</p>
     *
     * <p>Morphium auto-creates this index when the collection is first accessed or written
     * to, depending on the {@code autoIndexAndCappedCreationOnWrite} configuration.</p>
     */
    @Index
    private String department;

    /** Job title / position of the employee (e.g., "Senior Developer", "Tech Lead"). */
    private String position;

    /**
     * Annual salary. Used to demonstrate range queries with {@code gte()} and {@code lte()}
     * in {@link io.quarkiverse.morphium.showcase.query.QueryShowcaseService#findBySalaryRange}.
     */
    private double salary;

    /** Date when the employee was hired. Stored as a BSON DateTime in MongoDB. */
    private LocalDateTime hireDate;

    /**
     * Computed full name, NOT stored in MongoDB.
     *
     * <p>The {@code @Transient} annotation (from Morphium, not JPA) tells Morphium to
     * completely ignore this field during serialization and deserialization. It will never
     * be written to or read from MongoDB.</p>
     *
     * <p>The Java {@code transient} keyword is also applied as a secondary safeguard --
     * it tells the standard Java serialization mechanism to skip this field as well.
     * For Morphium, only the {@code @Transient} annotation is necessary, but using both
     * is good practice.</p>
     *
     * <p>This field is populated by the {@code @PostLoad} lifecycle callback below.</p>
     */
    @Transient
    private transient String fullName;

    /**
     * List of skills the employee possesses (e.g., "Java", "MongoDB", "Kubernetes").
     *
     * <p>The {@code @IgnoreNullFromDB} annotation tells Morphium: if this field is missing
     * or {@code null} in the MongoDB document, do NOT set the Java field to {@code null}.
     * Instead, keep whatever value the field currently has (typically its default or a
     * previously set value).</p>
     *
     * <p>This is particularly useful for List/Collection fields where you want to avoid
     * {@code NullPointerException}s. Without this annotation, if a document was stored
     * without a {@code skills} field, loading it would set {@code skills} to {@code null},
     * and any subsequent {@code skills.size()} call would fail.</p>
     *
     * <p>Also used in the Query showcase to demonstrate Morphium's {@code .in()} query
     * operator, which checks if the array contains any of the specified values.</p>
     */
    @IgnoreNullFromDB
    private List<String> skills;

    /** Whether the employee is currently active. Used to demonstrate boolean queries. */
    private boolean active;

    /**
     * Lifecycle callback invoked automatically by Morphium after this entity is loaded from MongoDB.
     *
     * <p>The {@code @PostLoad} annotation marks this method as a post-load callback. Morphium
     * calls it immediately after deserializing a MongoDB document into this Java object, but
     * BEFORE returning it to the calling code. This is the ideal place to compute derived
     * fields or perform validation on loaded data.</p>
     *
     * <p>Here, we compute the {@code fullName} transient field by concatenating first and last
     * name. Since {@code fullName} is {@code @Transient} and never stored in MongoDB, we must
     * recompute it every time the entity is loaded.</p>
     *
     * <p><b>Available lifecycle callbacks in Morphium:</b></p>
     * <ul>
     *   <li>{@code @PreStore} -- called before the entity is written to MongoDB</li>
     *   <li>{@code @PostStore} -- called after the entity has been successfully written</li>
     *   <li>{@code @PreLoad} -- called before a document is deserialized (rarely used)</li>
     *   <li>{@code @PostLoad} -- called after deserialization, as demonstrated here</li>
     *   <li>{@code @PreDelete} -- called before the entity is deleted</li>
     *   <li>{@code @PostDelete} -- called after the entity has been deleted</li>
     * </ul>
     *
     * <p><b>Prerequisite:</b> The class MUST be annotated with {@code @Lifecycle} for any
     * of these callbacks to be recognized and invoked by Morphium.</p>
     */
    @PostLoad
    public void onPostLoad() {
        this.fullName = firstName + " " + lastName;
    }
}
