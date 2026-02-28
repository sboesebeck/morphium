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
package io.quarkiverse.morphium.showcase.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.messaging.entity.ChatMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service layer demonstrating Morphium's core CRUD and query operations for a chat messaging system.
 *
 * <p>This service showcases the following Morphium features:</p>
 * <ul>
 *   <li><b>Query creation</b> -- Using {@code morphium.createQueryFor(Class)} to build type-safe queries</li>
 *   <li><b>Field filtering</b> -- Using {@code query.f(fieldName).eq(value)} for equality filters</li>
 *   <li><b>Sorting</b> -- Using {@code query.sort(Map.of(field, direction))} where 1 = ascending, -1 = descending</li>
 *   <li><b>Storing documents</b> -- Using {@code morphium.store(entity)} for insert/upsert operations</li>
 *   <li><b>Field-level updates</b> -- Using {@code query.set(field, value, ...)} to update a single field
 *       without loading/saving the entire document (translates to MongoDB's {@code $set} operator)</li>
 *   <li><b>Collection management</b> -- Using {@code morphium.dropCollection(Class)} to remove all documents</li>
 *   <li><b>Counting</b> -- Using {@code query.countAll()} for efficient server-side counting</li>
 * </ul>
 *
 * <p><b>CDI Integration:</b> The {@link Morphium} instance is injected by the Quarkus-Morphium extension.
 * It acts as the central entry point for all database operations -- similar to JPA's EntityManager but
 * tailored for MongoDB. One {@code Morphium} instance manages the connection pool, caching, and
 * write buffering for a single MongoDB deployment.</p>
 *
 * @see de.caluga.morphium.Morphium
 * @see ChatMessage
 */
@ApplicationScoped
public class MessagingService {

    @Inject
    Morphium morphium;

    /**
     * Retrieves all chat messages, sorted by send time in descending order (newest first).
     *
     * <p><b>How it works:</b></p>
     * <ol>
     *   <li>{@code createQueryFor(ChatMessage.class)} creates a new {@link de.caluga.morphium.query.Query}
     *       bound to the {@code chat_messages} collection (as defined by the entity's {@code @Entity} annotation).</li>
     *   <li>{@code sort(Map.of(field, -1))} adds a sort stage. The map key is the field name, and the
     *       value is the sort direction: {@code 1} for ascending, {@code -1} for descending.
     *       Here we use Lombok's generated {@code ChatMessage.Fields.sentAt} constant for type safety.</li>
     *   <li>{@code asList()} executes the query and returns all matching documents as a Java List.
     *       Morphium deserializes each BSON document back into a {@code ChatMessage} POJO.</li>
     * </ol>
     *
     * @return all chat messages sorted by send time descending
     */
    public List<ChatMessage> findAll() {
        // createQueryFor() builds a query targeting the collection mapped to ChatMessage
        // sort() with -1 means descending order (newest messages first)
        // asList() executes the query and returns the results as a Java List
        return morphium.createQueryFor(ChatMessage.class)
                .sort(Map.of(ChatMessage.Fields.sentAt, -1))
                .asList();
    }

    /**
     * Finds all messages belonging to a specific topic, sorted newest first.
     *
     * <p><b>Query filter chain:</b> The {@code .f(field).eq(value)} pattern is Morphium's
     * fluent API for building MongoDB query filters. {@code f()} selects a field, and
     * {@code eq()} adds an equality constraint. This translates to the MongoDB query
     * {@code {"topic": "<value>"}}.  Multiple {@code .f().eq()} calls on the same query
     * are combined with AND logic.</p>
     *
     * @param topic the topic/channel name to filter by
     * @return messages matching the given topic, sorted by send time descending
     */
    public List<ChatMessage> findByTopic(String topic) {
        // f() selects a field for filtering; eq() applies an equality match
        // This translates to MongoDB's { "topic": topic } query filter
        return morphium.createQueryFor(ChatMessage.class)
                .f(ChatMessage.Fields.topic).eq(topic)
                .sort(Map.of(ChatMessage.Fields.sentAt, -1))
                .asList();
    }

    /**
     * Creates and persists a new chat message.
     *
     * <p><b>morphium.store() behavior:</b></p>
     * <ul>
     *   <li>If the entity's {@code @Id} field is {@code null}, Morphium performs an INSERT and
     *       auto-generates a new {@link MorphiumId}. The generated ID is written back into the
     *       entity object, so after this call, {@code message.getId()} is non-null.</li>
     *   <li>If the {@code @Id} field is already set, Morphium performs an UPSERT (insert or
     *       replace). This means {@code store()} is idempotent for entities with a known ID.</li>
     *   <li>The {@code @CreationTime} field ({@code sentAt}) is automatically populated by
     *       Morphium during the first store, since it detects that the entity is new.</li>
     * </ul>
     *
     * @param sender    the username of the sender
     * @param recipient the username of the recipient
     * @param topic     the message topic/channel
     * @param text      the message body
     * @return the persisted ChatMessage with its generated ID and creation timestamp
     */
    public ChatMessage send(String sender, String recipient, String topic, String text) {
        ChatMessage message = ChatMessage.builder()
                .sender(sender)
                .recipient(recipient)
                .topic(topic)
                .text(text)
                .read(false)
                .build();
        // store() inserts the document (id is null) and auto-generates a MorphiumId.
        // The @CreationTime field (sentAt) is automatically set to the current time.
        morphium.store(message);
        return message;
    }

    /**
     * Marks a specific message as read using a targeted field-level update.
     *
     * <p><b>query.set() explained:</b> Instead of loading the full document, modifying it in Java,
     * and saving it back (which would be a full document replacement), {@code query.set()} sends a
     * MongoDB {@code $set} update command that modifies only the specified field. This is more
     * efficient and avoids race conditions in concurrent environments.</p>
     *
     * <p>The method signature is:
     * {@code set(String field, Object value, boolean upsert, boolean multiple, Enum<?> lifecycle)}</p>
     * <ul>
     *   <li>{@code field} -- the field name to update</li>
     *   <li>{@code value} -- the new value</li>
     *   <li>{@code upsert} ({@code false}) -- if true, would create the document if it does not exist</li>
     *   <li>{@code multiple} ({@code false}) -- if true, would update ALL matching documents (not just the first)</li>
     *   <li>{@code lifecycle} ({@code null}) -- optional lifecycle callback enum, not used here</li>
     * </ul>
     *
     * @param id the string representation of the message's MorphiumId
     */
    public void markAsRead(String id) {
        // Build a query that targets a single document by its _id
        var query = morphium.createQueryFor(ChatMessage.class)
                .f(ChatMessage.Fields.id).eq(new MorphiumId(id));
        // set() performs a MongoDB $set operation -- updates only the "read" field to true
        // Parameters: field, value, upsert=false, multiple=false, lifecycle=null
        query.set(ChatMessage.Fields.read, true, false, false, null);
    }

    /**
     * Deletes all chat messages by dropping the entire MongoDB collection.
     *
     * <p><b>dropCollection() vs. delete():</b> {@code dropCollection()} removes the entire
     * collection including all indexes and metadata. This is faster than deleting documents
     * one by one, but the collection (and its indexes) will be automatically recreated by
     * Morphium on the next write if {@code autoIndexAndCappedCreationOnWrite} is enabled.
     * Use this for test/demo reset scenarios; in production, prefer targeted deletes.</p>
     */
    public void deleteAll() {
        // dropCollection() removes the entire "chat_messages" collection from MongoDB.
        // The collection and its indexes are recreated automatically on the next write.
        morphium.dropCollection(ChatMessage.class);
    }

    /**
     * Counts all chat messages using a server-side count operation.
     *
     * <p><b>countAll()</b> translates to MongoDB's {@code countDocuments()} command, which
     * runs entirely on the server without transferring any documents to the client. This is
     * far more efficient than loading all documents and calling {@code .size()} on the list.</p>
     *
     * @return the total number of chat messages in the collection
     */
    public long count() {
        // countAll() executes a server-side count -- no documents are transferred to the client
        return morphium.createQueryFor(ChatMessage.class).countAll();
    }

    /**
     * Counts messages belonging to a specific topic.
     *
     * <p>Combines a field filter with {@code countAll()} to count only matching documents.
     * Translates to MongoDB's {@code countDocuments({"topic": "<value>"})}.</p>
     *
     * @param topic the topic to count messages for
     * @return the number of messages with the given topic
     */
    public long countByTopic(String topic) {
        return morphium.createQueryFor(ChatMessage.class)
                .f(ChatMessage.Fields.topic).eq(topic)
                .countAll();
    }

    /**
     * Returns a list of distinct topics across all messages.
     *
     * <p><b>Note:</b> This implementation loads all messages into memory and extracts
     * distinct topics in Java. For large datasets, consider using Morphium's
     * {@code query.distinct(fieldName)} method or MongoDB aggregation pipelines instead,
     * which perform the distinct operation on the server side.</p>
     *
     * @return a list of unique topic names
     */
    public List<String> getTopics() {
        // This loads all documents and extracts distinct topics in-memory.
        // For production use with large collections, prefer query.distinct("topic")
        // which executes the distinct operation on the MongoDB server.
        return morphium.createQueryFor(ChatMessage.class)
                .asList()
                .stream()
                .map(ChatMessage::getTopic)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Seeds the collection with sample chat messages for demonstration purposes.
     *
     * <p>The guard {@code if (count() > 0) return;} ensures idempotency -- seed data is
     * only inserted if the collection is empty. Each call to {@code send()} performs an
     * individual {@code store()} operation. For bulk inserts, consider using
     * {@code morphium.storeList()} which batches multiple inserts into a single
     * MongoDB bulk write for better performance (see the Importer module for an example).</p>
     */
    public void seedData() {
        if (count() > 0) return;

        send("Alice", "Bob", "general", "Hey Bob, how is the project going?");
        send("Bob", "Alice", "general", "Hi Alice! It's going well, almost done with the backend.");
        send("Charlie", "Alice", "tech-support", "Alice, I need help with the deployment pipeline.");
        send("Alice", "Charlie", "tech-support", "Sure Charlie, let me take a look at the CI config.");
        send("Bob", "Charlie", "general", "Charlie, want to join the standup tomorrow?");
    }
}
