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
package io.quarkiverse.morphium.showcase.importer;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.SequenceGenerator;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import io.quarkiverse.morphium.showcase.importer.entity.ImportRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service demonstrating Morphium's bulk write, sequence generation, and array manipulation features.
 *
 * <p>This service is a comprehensive showcase of Morphium's advanced write operations:</p>
 * <ul>
 *   <li><b>{@link SequenceGenerator}</b> -- MongoDB-backed atomic sequence counter for generating
 *       monotonically increasing IDs (similar to SQL auto-increment)</li>
 *   <li><b>{@code morphium.storeList()}</b> -- Bulk insert of multiple documents in a single
 *       MongoDB bulk write operation for maximum throughput</li>
 *   <li><b>{@code morphium.push()}</b> -- Appends a value to an array field without loading
 *       the document (MongoDB's {@code $push} operator)</li>
 *   <li><b>{@code morphium.pull()}</b> -- Removes a value from an array field without loading
 *       the document (MongoDB's {@code $pull} operator)</li>
 *   <li><b>{@code query.set()}</b> -- Updates a single field in-place (MongoDB's {@code $set})</li>
 *   <li><b>{@code query.unset()}</b> -- Removes a field entirely from the document (MongoDB's {@code $unset})</li>
 *   <li><b>{@code morphium.dropCollection()}</b> -- Drops the entire collection</li>
 * </ul>
 *
 * <p><b>Performance note:</b> The {@code ImportRecord} entity is annotated with
 * {@code @WriteBuffer(size=500, timeout=5000)}, so individual {@code store()} calls are
 * buffered and batched automatically. However, {@code storeList()} bypasses the write buffer
 * and sends the bulk write directly, which is the preferred approach for known batch imports.</p>
 *
 * @see de.caluga.morphium.SequenceGenerator
 * @see ImportRecord
 */
@ApplicationScoped
public class ImportService {

    @Inject
    Morphium morphium;

    /**
     * Retrieves import records with a limit, sorted by import number descending (newest first).
     *
     * <p>Demonstrates Morphium's {@code limit()} method which restricts the number of documents
     * returned. This translates directly to MongoDB's {@code limit} cursor modifier, ensuring
     * only the specified number of documents are transferred from the server.</p>
     *
     * @param limit maximum number of records to return
     * @return the most recent import records, up to the specified limit
     */
    public List<ImportRecord> findAll(int limit) {
        // sort() with -1 means descending order (highest import number first)
        // limit() restricts the result set size on the server side
        return morphium.createQueryFor(ImportRecord.class)
                .sort(Map.of(ImportRecord.Fields.importNumber, -1))
                .limit(limit)
                .asList();
    }

    /**
     * Counts all import records using a server-side count operation.
     *
     * @return the total number of import records in the collection
     */
    public long count() {
        return morphium.createQueryFor(ImportRecord.class).countAll();
    }

    /**
     * Performs a bulk import of records, demonstrating SequenceGenerator and storeList().
     *
     * <h3>SequenceGenerator</h3>
     * <p>{@link SequenceGenerator} provides atomic, MongoDB-backed sequence numbers -- similar
     * to SQL's AUTO_INCREMENT but in a distributed NoSQL environment. It works by maintaining
     * a dedicated MongoDB document that stores the current counter value and uses MongoDB's
     * atomic {@code findAndModify} to increment it safely even under concurrent access.</p>
     *
     * <p>Constructor parameters:</p>
     * <ul>
     *   <li>{@code morphium} -- the Morphium instance to use</li>
     *   <li>{@code "import_number"} -- the sequence name (allows multiple independent sequences)</li>
     *   <li>{@code 1} -- the starting value for a new sequence</li>
     *   <li>{@code 1} -- the increment step per call to {@code getNextValue()}</li>
     * </ul>
     *
     * <h3>storeList()</h3>
     * <p>{@code morphium.storeList(records)} sends all entities to MongoDB in a single bulk
     * write operation. This is dramatically faster than calling {@code store()} for each entity
     * individually because:</p>
     * <ul>
     *   <li>It requires only one network round-trip instead of N round-trips</li>
     *   <li>MongoDB processes bulk writes more efficiently than individual inserts</li>
     *   <li>It bypasses the {@code @WriteBuffer} -- records go directly to MongoDB</li>
     * </ul>
     *
     * @param count the number of records to generate and import
     * @return the elapsed time in milliseconds for the bulk import operation
     */
    public long bulkImport(int count) {
        // SequenceGenerator creates a MongoDB-backed atomic counter.
        // Each call to getNextValue() atomically increments and returns the next number.
        // The sequence state is stored in a dedicated MongoDB collection ("sequence" by default).
        SequenceGenerator seq = new SequenceGenerator(morphium, "import_number", 1, 1);
        List<ImportRecord> records = new ArrayList<>();
        String[] sources = {"API", "CSV", "FTP", "S3", "MANUAL"};
        String[] statuses = {"PENDING", "PROCESSED", "ERROR"};

        long start = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            records.add(ImportRecord.builder()
                    // getNextValue() performs an atomic findAndModify on the sequence document,
                    // guaranteeing unique, monotonically increasing values even under concurrency
                    .importNumber(seq.getNextValue())
                    .source(sources[i % sources.length])
                    .data("Bulk import record #" + (i + 1))
                    .status(statuses[i % statuses.length])
                    .tags(List.of("bulk", "auto-generated"))
                    .build());
        }

        // storeList() performs a single MongoDB bulk write for all records.
        // This is much faster than calling store() individually for each record.
        // Note: storeList() bypasses the @WriteBuffer and writes directly to MongoDB.
        morphium.storeList(records);

        return System.currentTimeMillis() - start;
    }

    /**
     * Finds import records by their processing status.
     *
     * <p>Uses the standard Morphium query pattern: {@code f(field).eq(value)}
     * which translates to {@code {"status": "<value>"}} in MongoDB.</p>
     *
     * @param status the status to filter by (e.g., "PENDING", "PROCESSED", "ERROR")
     * @return all import records matching the given status
     */
    public List<ImportRecord> findByStatus(String status) {
        return morphium.createQueryFor(ImportRecord.class)
                .f(ImportRecord.Fields.status).eq(status)
                .asList();
    }

    /**
     * Adds a tag to an import record's tags array using MongoDB's {@code $push} operator.
     *
     * <p><b>morphium.push()</b> appends a value to an array field directly on the MongoDB
     * server. The document is never loaded into Java memory -- Morphium sends an update
     * command like: {@code db.import_records.updateOne({_id: id}, {$push: {tags: "newTag"}})}</p>
     *
     * <p>This is more efficient and safer than loading the document, modifying the list
     * in Java, and saving it back, which could overwrite concurrent changes to other fields.</p>
     *
     * @param id  the string representation of the record's MorphiumId
     * @param tag the tag to add to the tags array
     */
    public void addTag(String id, String tag) {
        Query<ImportRecord> query = morphium.createQueryFor(ImportRecord.class)
                .f(ImportRecord.Fields.id).eq(new MorphiumId(id));
        // push() appends "tag" to the "tags" array field using MongoDB's $push operator.
        // The document is NOT loaded -- the update happens entirely on the server.
        morphium.push(query, ImportRecord.Fields.tags, tag);
    }

    /**
     * Removes a tag from an import record's tags array using MongoDB's {@code $pull} operator.
     *
     * <p><b>morphium.pull()</b> removes all occurrences of the specified value from the array
     * field. Like {@code push()}, this operates directly on the server without loading the
     * document. Translates to: {@code db.import_records.updateOne({_id: id}, {$pull: {tags: "oldTag"}})}</p>
     *
     * @param id  the string representation of the record's MorphiumId
     * @param tag the tag to remove from the tags array
     */
    public void removeTag(String id, String tag) {
        Query<ImportRecord> query = morphium.createQueryFor(ImportRecord.class)
                .f(ImportRecord.Fields.id).eq(new MorphiumId(id));
        // pull() removes all occurrences of "tag" from the "tags" array using MongoDB's $pull operator.
        morphium.pull(query, ImportRecord.Fields.tags, tag);
    }

    /**
     * Updates an import record's status to "PROCESSED" using a targeted field-level update.
     *
     * <p><b>query.set()</b> performs a MongoDB {@code $set} operation that modifies only the
     * specified field without touching any other fields in the document. This is the equivalent
     * of: {@code db.import_records.updateOne({_id: id}, {$set: {status: "PROCESSED"}})}</p>
     *
     * <p>Parameters: {@code set(field, value, upsert, multiple, lifecycle)}</p>
     * <ul>
     *   <li>{@code upsert=false} -- do not create the document if it does not exist</li>
     *   <li>{@code multiple=false} -- update only the first matching document</li>
     *   <li>{@code lifecycle=null} -- no lifecycle callback</li>
     * </ul>
     *
     * @param id the string representation of the record's MorphiumId
     */
    public void markProcessed(String id) {
        Query<ImportRecord> query = morphium.createQueryFor(ImportRecord.class)
                .f(ImportRecord.Fields.id).eq(new MorphiumId(id));
        // set() updates only the "status" field to "PROCESSED" using MongoDB's $set operator.
        // The rest of the document remains untouched.
        query.set(ImportRecord.Fields.status, "PROCESSED", false, false, null);
    }

    /**
     * Removes the "source" field entirely from an import record using MongoDB's {@code $unset} operator.
     *
     * <p><b>query.unset()</b> is different from setting a field to {@code null}. While
     * {@code set(field, null)} would store a null value in the document, {@code unset(field)}
     * completely removes the field from the BSON document. After unset, the field will not
     * appear in the document at all -- not even as {@code null}.</p>
     *
     * <p>This is useful for schema evolution, where you want to remove deprecated fields from
     * documents without rewriting them entirely.</p>
     *
     * @param id the string representation of the record's MorphiumId
     */
    public void unsetSource(String id) {
        Query<ImportRecord> query = morphium.createQueryFor(ImportRecord.class)
                .f(ImportRecord.Fields.id).eq(new MorphiumId(id));
        // unset() removes the "source" field entirely from the BSON document.
        // This is different from set(field, null) -- unset removes the key altogether.
        query.unset(ImportRecord.Fields.source);
    }

    /**
     * Deletes all import records by dropping the entire collection.
     *
     * <p>{@code dropCollection()} removes the collection, all its documents, and all its indexes.
     * Morphium will auto-recreate the collection (and reapply indexes) on the next write.</p>
     */
    public void deleteAll() {
        morphium.dropCollection(ImportRecord.class);
    }

    /**
     * Seeds the collection with 20 sample import records for demonstration purposes.
     *
     * <p>Uses {@link SequenceGenerator} for monotonically increasing import numbers
     * and {@code morphium.storeList()} to persist all records in a single bulk operation.
     * The guard {@code if (count() > 0) return;} ensures idempotency.</p>
     */
    public void seedData() {
        if (count() > 0) return;

        // Create a SequenceGenerator named "import_number" starting at 1 with step 1
        SequenceGenerator seq = new SequenceGenerator(morphium, "import_number", 1, 1);

        String[] sources = {"API", "CSV", "FTP", "S3", "MANUAL"};
        String[] statuses = {"PENDING", "PROCESSED", "ERROR"};
        List<ImportRecord> records = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            records.add(ImportRecord.builder()
                    .importNumber(seq.getNextValue())
                    .source(sources[i % sources.length])
                    .data("Sample data record #" + (i + 1))
                    .status(statuses[i % statuses.length])
                    .tags(List.of("seed", sources[i % sources.length].toLowerCase()))
                    .build());
        }

        // Bulk insert all 20 records in a single MongoDB operation
        morphium.storeList(records);
    }
}
