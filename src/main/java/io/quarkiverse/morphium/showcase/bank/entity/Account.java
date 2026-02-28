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
package io.quarkiverse.morphium.showcase.bank.entity;

import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;

/**
 * Morphium entity representing a bank account, demonstrating optimistic locking and automatic timestamps.
 *
 * <p>This entity showcases three important Morphium features beyond basic entity mapping:</p>
 *
 * <ul>
 *   <li><strong>{@code @Version}</strong> -- Enables optimistic locking. Morphium automatically increments
 *       this field on every {@code store()} call and checks that the version in the database matches the
 *       version in the Java object before writing. If another thread/process modified the document in the
 *       meantime (version mismatch), Morphium throws a {@code VersionMismatchException}. This prevents
 *       lost updates without requiring pessimistic locks.</li>
 *   <li><strong>{@code @CreationTime}</strong> -- Morphium automatically sets this field to the current
 *       timestamp when the document is first stored. On subsequent updates, the value is not changed.
 *       This is useful for audit trails and "created at" timestamps without manual code.</li>
 *   <li><strong>{@code @Index(options = {"unique:1"})}</strong> -- Creates a unique index on the field,
 *       ensuring no two documents can have the same value. MongoDB will reject duplicate inserts.</li>
 * </ul>
 *
 * <h3>How optimistic locking works in Morphium</h3>
 * <ol>
 *   <li>Read an Account -- the {@code version} field is, say, 3.</li>
 *   <li>Modify the Account in Java (e.g., change the balance).</li>
 *   <li>Call {@code morphium.store(account)} -- Morphium adds a condition: {@code WHERE _id = ... AND version = 3}.</li>
 *   <li>If the update matches 1 document, it succeeds and version becomes 4.</li>
 *   <li>If the update matches 0 documents (someone else already updated it), Morphium throws
 *       {@code VersionMismatchException}.</li>
 * </ol>
 */
// @Entity: Maps this class to the "accounts" MongoDB collection.
@Entity(collectionName = "accounts")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class Account {

    /**
     * The MongoDB document {@code _id} field.
     *
     * <p>{@code @Id} marks this as the primary key. {@link MorphiumId} is Morphium's ObjectId
     * implementation, auto-generated on first store if null.</p>
     */
    @Id
    private MorphiumId id;

    /**
     * A unique account number (e.g., "ACC-001").
     *
     * <p>{@code @Index(options = {"unique:1"})} creates a unique index in MongoDB. Any attempt to
     * insert a second document with the same {@code accountNumber} will fail with a duplicate key
     * error. The {@code options} array uses Morphium's key-value format to pass MongoDB index
     * options.</p>
     */
    // @Index with unique:1 ensures MongoDB enforces uniqueness at the database level.
    // This is stronger than application-level checks because it works even with concurrent inserts.
    @Index(options = {"unique:1"})
    private String accountNumber;

    /** The name of the account owner. Stored as a plain string field in MongoDB. */
    private String ownerName;

    /**
     * The current account balance.
     *
     * <p>In the {@link io.quarkiverse.morphium.showcase.bank.BankService}, balance updates use
     * Morphium's atomic {@code inc()} operation rather than read-modify-write, which avoids
     * race conditions even without transactions.</p>
     */
    private double balance;

    /** The currency code (defaults to "EUR"). Stored as a plain string field. */
    @Builder.Default
    private String currency = "EUR";

    /**
     * The optimistic locking version counter.
     *
     * <p>{@code @Version} tells Morphium to manage this field automatically:</p>
     * <ul>
     *   <li>On first store: set to 1</li>
     *   <li>On subsequent stores: increment by 1, but only if the current database version matches</li>
     *   <li>On version mismatch: throw {@code VersionMismatchException} instead of overwriting</li>
     * </ul>
     * <p>The field type must be {@code Long} (boxed), not {@code long} (primitive), so that Morphium
     * can detect the initial null state for new entities.</p>
     */
    // @Version: Morphium's optimistic locking mechanism. Every store() increments this value and
    // conditionally updates only if the DB version matches. Prevents lost updates in concurrent scenarios.
    @Version
    private Long version;

    /**
     * Timestamp of when this account was first created.
     *
     * <p>{@code @CreationTime} is a Morphium lifecycle annotation. Morphium automatically sets this
     * field to the current date/time when the entity is first stored (inserted). On subsequent updates,
     * the value remains unchanged. Morphium supports both {@code java.util.Date} and
     * {@code java.time.LocalDateTime} as field types for this annotation.</p>
     */
    // @CreationTime: Automatically populated by Morphium on the first store() call.
    // Subsequent updates do NOT overwrite this value -- it preserves the original creation timestamp.
    @CreationTime
    private LocalDateTime createdAt;
}