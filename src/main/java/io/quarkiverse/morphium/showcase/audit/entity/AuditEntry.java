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
package io.quarkiverse.morphium.showcase.audit.entity;

import de.caluga.morphium.annotations.Capped;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;

/**
 * Morphium entity demonstrating <strong>Capped Collections</strong> and the
 * <strong>{@code @CreationTime}</strong> automatic timestamp annotation.
 *
 * <h3>Key Morphium Features Demonstrated</h3>
 * <ul>
 *   <li>{@code @Entity(collectionName = "audit_log")} -- Maps this class to the {@code audit_log}
 *       MongoDB collection. The explicit {@code collectionName} overrides Morphium's default
 *       naming (which would produce something like {@code audit_entry}).</li>
 *   <li>{@code @Capped} -- Instructs Morphium to create this collection as a
 *       <strong>capped collection</strong>. Capped collections are fixed-size, circular-buffer
 *       collections that automatically discard the oldest documents when the limit is reached.
 *       They guarantee insertion order and are ideal for use cases like audit logs, event streams,
 *       and cache-like scenarios.
 *       <ul>
 *         <li>{@code maxSize = 10485760} -- Maximum size in bytes (10 MB). MongoDB requires
 *             at least this parameter for capped collections.</li>
 *         <li>{@code maxEntries = 10000} -- Maximum number of documents. When exceeded, the
 *             oldest documents are automatically removed (FIFO). This parameter is optional
 *             and is in addition to the size constraint.</li>
 *       </ul>
 *       <strong>Important:</strong> Morphium checks and creates capped collections automatically
 *       when {@code autoIndexAndCappedCreationOnWrite} is enabled (default). You cannot convert
 *       an existing non-capped collection to capped -- you must drop it first.</li>
 *   <li>{@code @CreationTime} -- Morphium automatically sets this field to the current timestamp
 *       when the document is first stored. This happens in the Morphium framework <em>before</em>
 *       the write to MongoDB, so the timestamp reflects the application server's time. Unlike
 *       MongoDB's {@code $currentDate} operator, this is set client-side by Morphium. The field
 *       is only set on initial creation, not on subsequent updates.</li>
 *   <li>{@code @Id} -- The mandatory primary key field mapped to MongoDB's {@code _id}.</li>
 * </ul>
 *
 * <h3>Why Capped Collections for Audit Logs?</h3>
 * <p>Capped collections are a natural fit for audit/event logs because:</p>
 * <ul>
 *   <li>They automatically manage storage by discarding old entries (no manual cleanup needed).</li>
 *   <li>They maintain insertion order, making chronological queries efficient.</li>
 *   <li>Writes are very fast because MongoDB does not need to update indexes for deletion.</li>
 *   <li>They support tailable cursors for real-time streaming of new documents.</li>
 * </ul>
 * <p>However, capped collections have restrictions: you cannot delete individual documents,
 * and updates that would increase a document's size are not allowed.</p>
 *
 * @see io.quarkiverse.morphium.showcase.audit.AuditListener AuditListener for how entries are stored
 * @see io.quarkiverse.morphium.showcase.audit.AuditResource AuditResource for querying entries
 */
// @Entity -- Marks this class as a Morphium entity mapped to the "audit_log" collection.
@Entity(collectionName = "audit_log")

// @Capped -- Makes MongoDB create this collection as a capped (fixed-size, FIFO) collection.
// maxSize: 10 MB hard limit on collection size in bytes.
// maxEntries: 10,000 document limit (whichever limit is hit first triggers eviction).
// Morphium will create the capped collection on first write if it does not already exist.
@Capped(maxSize = 10485760, maxEntries = 10000)

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class AuditEntry {

    /**
     * The unique document identifier, mapped to MongoDB's {@code _id}.
     *
     * <p>Using {@link MorphiumId} provides a MongoDB ObjectId-compatible identifier.
     * When left {@code null} (as in the builder pattern used by {@code AuditListener}),
     * Morphium auto-generates the ID on store.</p>
     */
    @Id
    private MorphiumId id;

    /**
     * The type of entity that was modified (e.g., "Product", "Account", "BlogPost").
     * Stored as {@code entity_type} in MongoDB due to Morphium's default camelCase-to-snake_case mapping.
     */
    private String entityType;

    /**
     * The identifier of the specific entity instance that was modified (e.g., "product-42").
     * Stored as {@code entity_id} in MongoDB.
     */
    private String entityId;

    /**
     * Action performed: CREATE, UPDATE, or DELETE.
     *
     * <p>This is a plain String field -- no special Morphium annotation needed. Morphium
     * persists all non-transient, non-static fields by default.</p>
     */
    private String action;

    /**
     * Human-readable description of what changed.
     */
    private String details;

    /**
     * The user or system principal that performed the action.
     */
    private String user;

    /**
     * The timestamp when this audit entry was created.
     *
     * <p>{@code @CreationTime} is a Morphium lifecycle annotation that automatically populates
     * this field with the current date/time when the entity is <em>first stored</em>. Key behaviors:</p>
     * <ul>
     *   <li>The timestamp is set client-side by Morphium (not by MongoDB's server clock).</li>
     *   <li>It is only set on creation, not on subsequent updates -- making it a true
     *       "created at" timestamp.</li>
     *   <li>Supported types: {@code long} (epoch millis), {@code Date}, and {@code LocalDateTime}.</li>
     *   <li>If you need a "last modified" timestamp instead, use {@code @LastChange}.</li>
     * </ul>
     */
    @CreationTime
    private LocalDateTime timestamp;
}