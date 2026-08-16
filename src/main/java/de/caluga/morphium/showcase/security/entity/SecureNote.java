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
package de.caluga.morphium.showcase.security.entity;

import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.time.LocalDateTime;

/**
 * Morphium entity demonstrating <strong>Field-Level Encryption</strong> via
 * {@code @Encrypted}.
 *
 * <h3>Key Morphium Feature Demonstrated</h3>
 * <p>{@code @Encrypted(provider = AESEncryptionProvider.class, keyName = "secure-note-key")}
 * on the {@link #secretContent} field. This annotation is entirely transparent to the rest of
 * the application: {@code ObjectMapperImpl} intercepts serialization and deserialization for
 * annotated fields.</p>
 *
 * <h3>What Actually Happens</h3>
 * <ul>
 *   <li><strong>On {@code morphium.store(note)}</strong>: {@code ObjectMapperImpl} looks up the
 *       encryption key for {@code keyName()} via
 *       {@code morphium.getEncryptionKeyProvider().getEncryptionKey(keyName)}, encrypts the
 *       field's serialized bytes with the configured {@code ValueEncryptionProvider} (here:
 *       {@link AESEncryptionProvider}), and stores the result as a raw {@code byte[]} in the
 *       BSON document -- <em>never</em> as a String. If you inspect the MongoDB document
 *       directly (bypassing Morphium's object mapper), you will see a byte array, not
 *       readable text.</li>
 *   <li><strong>On query/{@code get()}/{@code asList()}</strong>: {@code ObjectMapperImpl}
 *       detects the {@code @Encrypted} annotation during deserialization, decrypts the stored
 *       bytes using {@code getDecryptionKey(keyName)}, and hands back the original plaintext
 *       String. The application code never sees the byte array.</li>
 * </ul>
 *
 * <p><strong>Important:</strong> the encryption/decryption keys must be registered on
 * {@code morphium.getEncryptionKeyProvider()} <em>before</em> the first {@code store()} or
 * query -- see {@link de.caluga.morphium.showcase.security.EncryptionService#ensureKeys()}.</p>
 *
 * @see de.caluga.morphium.showcase.security.EncryptionService
 */
// @Entity: maps this class to the "secure_notes" MongoDB collection.
@Entity(collectionName = "secure_notes")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class SecureNote {

    /** MongoDB {@code _id}, auto-generated on first store. */
    @Id
    private MorphiumId id;

    /** Plaintext title -- NOT encrypted, so it remains searchable/indexable as-is. */
    private String title;

    /**
     * The sensitive payload of this note (e.g. a secret, an SSN-like value, API credentials).
     *
     * <p>{@code @Encrypted(provider = AESEncryptionProvider.class, keyName = "secure-note-key")}
     * -- Morphium transparently encrypts this field's value before it is written to MongoDB, and
     * transparently decrypts it again when the entity is loaded. The {@code keyName} identifies
     * which key (registered via {@code EncryptionKeyProvider}) to use; multiple entities/fields
     * can share a key name or each use their own.</p>
     */
    @Encrypted(provider = AESEncryptionProvider.class, keyName = "secure-note-key")
    private String secretContent;

    /** Plaintext category label -- not encrypted, used for filtering in the demo. */
    private String category;

    /** Auto-populated by Morphium on first store(); never overwritten afterwards. */
    @CreationTime
    private LocalDateTime createdAt;
}
