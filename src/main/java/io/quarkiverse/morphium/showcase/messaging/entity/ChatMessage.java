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
package io.quarkiverse.morphium.showcase.messaging.entity;

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
 * Morphium entity representing a chat message stored in MongoDB.
 *
 * <p>This entity demonstrates several core Morphium ORM features:</p>
 * <ul>
 *   <li><b>{@code @Entity}</b> -- Marks this class as a Morphium-managed document.
 *       The {@code collectionName} attribute explicitly sets the MongoDB collection name
 *       to {@code "chat_messages"}. Without this attribute, Morphium would derive the
 *       collection name from the class name using its naming convention (lowercase with
 *       underscores, e.g., {@code "chat_message"}).</li>
 *   <li><b>{@code @Id}</b> -- Designates the primary key field. Morphium maps this to
 *       MongoDB's {@code _id} field. The type {@link MorphiumId} is Morphium's own
 *       ObjectId implementation, compatible with MongoDB's native ObjectId. If no value
 *       is set before storing, Morphium auto-generates one.</li>
 *   <li><b>{@code @CreationTime}</b> -- Automatically populated by Morphium with the
 *       current timestamp when the document is first stored (inserted). On subsequent
 *       updates, this field is NOT modified -- it captures the original creation time only.
 *       Supports {@link java.util.Date}, {@code long}, and {@link LocalDateTime}.</li>
 * </ul>
 *
 * <p><b>Lombok integration:</b> The {@code @FieldNameConstants} annotation generates a
 * static inner class {@code Fields} with string constants for each field name (e.g.,
 * {@code ChatMessage.Fields.sentAt}). This is heavily used in Morphium queries to
 * provide type-safe field references instead of error-prone string literals.</p>
 *
 * @see de.caluga.morphium.annotations.Entity
 * @see de.caluga.morphium.annotations.Id
 * @see de.caluga.morphium.annotations.CreationTime
 */
@Entity(collectionName = "chat_messages")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class ChatMessage {

    /**
     * Unique identifier for this chat message, mapped to MongoDB's {@code _id} field.
     *
     * <p>{@link MorphiumId} is Morphium's equivalent of MongoDB's ObjectId. It encodes
     * a timestamp, machine identifier, process ID, and a counter -- making it globally
     * unique without requiring a central sequence generator. Morphium auto-assigns this
     * value on first store if it is {@code null}.</p>
     */
    @Id
    private MorphiumId id;

    /** The username of the person who sent this message. */
    private String sender;

    /** The username of the intended recipient. */
    private String recipient;

    /**
     * Topic or channel this message belongs to (e.g., "general", "tech-support").
     * Used for filtering messages by topic via Morphium queries.
     */
    private String topic;

    /** The actual message content / body text. */
    private String text;

    /**
     * Timestamp automatically set by Morphium when this document is first persisted.
     *
     * <p>The {@code @CreationTime} annotation tells Morphium to inject the current
     * date/time into this field during the first {@code store()} call. Internally,
     * Morphium registers a lifecycle callback that checks whether the entity is new
     * (has no {@code _id} yet) and, if so, sets this field. On updates to an existing
     * document, the creation time remains unchanged.</p>
     *
     * <p>Morphium supports {@link LocalDateTime} (as used here), {@link java.util.Date},
     * and {@code long} (epoch millis) for {@code @CreationTime} fields.</p>
     */
    @CreationTime
    private LocalDateTime sentAt;

    /**
     * Whether this message has been read by the recipient.
     * Demonstrates how Morphium's {@code query.set()} can perform targeted field-level
     * updates without rewriting the entire document (see {@code MessagingService#markAsRead}).
     */
    private boolean read;
}
