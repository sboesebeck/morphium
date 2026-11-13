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
package de.caluga.morphium.showcase.security;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.showcase.security.entity.SecureNote;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Service layer demonstrating Morphium's <strong>Field-Level Encryption</strong> feature.
 *
 * <p>This class showcases the full lifecycle: registering encryption/decryption keys,
 * storing an entity with an {@code @Encrypted} field, and proving -- by reading the
 * <em>raw</em> MongoDB document via the low-level driver, bypassing Morphium's object
 * mapper -- that the field is genuinely stored as ciphertext (a {@code byte[]}), not as
 * readable text.</p>
 *
 * <h3>Why the Raw-Document Read Matters</h3>
 * <p>{@code morphium.createQueryFor(SecureNote.class).get()} always returns the
 * transparently-decrypted plaintext -- by design, that is the whole point of the feature.
 * That means a normal query can <em>never</em> be used to demonstrate that data is actually
 * encrypted in the database. To prove it, this service issues a {@link FindCommand} directly
 * against {@link Morphium#getDriver()}, which returns the document exactly as MongoDB stores
 * it, with the {@code secretContent} field still a {@code byte[]}.</p>
 */
@ApplicationScoped
public class EncryptionService {

    /** The key name shared by {@link SecureNote#getSecretContent()}'s {@code @Encrypted} annotation. */
    public static final String KEY_NAME = "secure-note-key";

    /**
     * A fixed demo key. In production this MUST come from a secrets manager / KMS, never be
     * hardcoded. AES here uses a 16-byte key (128-bit) matching {@code de.caluga.rsa.AES}'s
     * expectations, as shown in Morphium's own {@code EncryptedObjectMappingTests}.
     */
    private static final byte[] DEMO_KEY = "1234567890abcdef".getBytes(StandardCharsets.UTF_8);

    @Inject
    Morphium morphium;

    /**
     * Registers the AES encryption/decryption key on Morphium's {@code EncryptionKeyProvider}.
     *
     * <p><strong>Must run before the first {@code store()}/query</strong> that touches an
     * {@code @Encrypted} field -- otherwise {@code ObjectMapperImpl} has no key to
     * encrypt/decrypt with and will fail. {@code @PostConstruct} guarantees this happens once
     * this CDI bean is first instantiated, which -- since {@link SecurityResource} injects this
     * service -- is before any request touches the demo.</p>
     */
    @PostConstruct
    void ensureKeys() {
        morphium.getEncryptionKeyProvider().setEncryptionKey(KEY_NAME, DEMO_KEY);
        morphium.getEncryptionKeyProvider().setDecryptionKey(KEY_NAME, DEMO_KEY);
    }

    /**
     * Stores a new {@link SecureNote}. Because {@code secretContent} carries
     * {@code @Encrypted(keyName = "secure-note-key")}, Morphium's {@code ObjectMapperImpl}
     * encrypts it to a {@code byte[]} as part of this single {@code store()} call -- no
     * separate encryption step is required in application code.
     *
     * @param title         plaintext title (not encrypted)
     * @param secretContent the sensitive value to encrypt at rest
     * @param category      plaintext category label (not encrypted)
     * @return the stored note, with {@code id} populated
     */
    public SecureNote createNote(String title, String secretContent, String category) {
        SecureNote note = SecureNote.builder()
                .title(title)
                .secretContent(secretContent)
                .category(category)
                .build();
        // store() -- ObjectMapperImpl transparently encrypts secretContent before this hits MongoDB.
        morphium.store(note);
        return note;
    }

    /** Returns all notes, sorted newest first. Values arrive already decrypted. */
    public List<SecureNote> findAll() {
        // createQueryFor(...).asList() -- ObjectMapperImpl transparently decrypts secretContent
        // while deserializing each document. The application never sees the raw bytes here.
        return morphium.createQueryFor(SecureNote.class)
                .sort(Map.of(SecureNote.Fields.createdAt, -1))
                .asList();
    }

    public long count() {
        return morphium.createQueryFor(SecureNote.class).countAll();
    }

    public void deleteAll() {
        morphium.dropCollection(SecureNote.class);
    }

    /**
     * Reads the raw MongoDB document for the given note id, bypassing Morphium's object mapper
     * entirely, to prove that {@code secretContent} is genuinely stored as ciphertext.
     *
     * <p>A {@link FindCommand} is issued directly against a connection obtained from
     * {@link Morphium#getDriver()}. Unlike {@code morphium.createQueryFor(...)}, this command
     * has no knowledge of {@code @Encrypted} and returns the document fields exactly as stored
     * in the collection -- so {@code secretContent} comes back as a {@code byte[]} (or a
     * driver-specific binary representation of one), not a String.</p>
     *
     * @param note a previously-stored note (must have a non-null id)
     * @return the raw MongoDB document as a {@code Map}, or {@code null} if not found
     */
    public Map<String, Object> readRawDocument(SecureNote note) {
        try {
            FindCommand fc = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
                    .setDb(morphium.getDatabase())
                    .setColl(morphium.getMapper().getCollectionName(SecureNote.class))
                    .setFilter(Map.of("_id", note.getId()));
            try {
                List<Map<String, Object>> raw = fc.execute();
                return raw.isEmpty() ? null : raw.get(0);
            } finally {
                fc.releaseConnection();
            }
        } catch (MorphiumDriverException e) {
            throw new RuntimeException("Failed to read raw document for note " + note.getId(), e);
        }
    }

    /**
     * Renders a value from the raw document for display, describing whether it looks like
     * ciphertext (a binary/byte-array value) or plain text.
     */
    public String describeRawValue(Object rawValue) {
        if (rawValue == null) {
            return "(no value)";
        }
        if (rawValue instanceof byte[] bytes) {
            return "byte[" + bytes.length + "] (ciphertext, not human-readable) = " + bytesToHex(bytes);
        }
        // Some drivers may hand back a Binary-ish wrapper or a Base64 string representation --
        // still not the plaintext, so show it as-is for transparency.
        return rawValue + " (type: " + rawValue.getClass().getSimpleName() + ")";
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int max = Math.min(bytes.length, 32);
        for (int i = 0; i < max; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        if (bytes.length > max) {
            sb.append("...");
        }
        return sb.toString();
    }
}
