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
package de.caluga.morphium.showcase;

import de.caluga.morphium.showcase.security.EncryptionService;
import de.caluga.morphium.showcase.security.entity.SecureNote;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the field-level encryption showcase feature: the plaintext value round-trips through
 * a normal Morphium query, while the raw MongoDB document genuinely stores ciphertext (a
 * {@code byte[]}), not the plaintext string.
 */
@QuarkusTest
class EncryptionServiceTest {

    @Inject
    EncryptionService encryptionService;

    @BeforeEach
    void setUp() {
        encryptionService.deleteAll();
    }

    @Test
    void queryReturnsTransparentlyDecryptedPlaintext() {
        SecureNote note = encryptionService.createNote("API Key", "sk-live-super-secret-123", "api-keys");

        SecureNote loaded = encryptionService.findAll().stream()
                .filter(n -> n.getId().equals(note.getId()))
                .findFirst().orElseThrow();

        assertEquals("sk-live-super-secret-123", loaded.getSecretContent());
        assertEquals("API Key", loaded.getTitle());
    }

    @Test
    void rawMongoDocumentStoresCiphertextNotPlaintext() {
        SecureNote note = encryptionService.createNote("API Key", "sk-live-super-secret-123", "api-keys");

        Map<String, Object> raw = encryptionService.readRawDocument(note);

        assertNotNull(raw);
        Object rawSecret = raw.get("secret_content");
        assertInstanceOf(byte[].class, rawSecret,
                "the raw stored value must be ciphertext bytes, not the plaintext string");
        assertFalse(new String((byte[]) rawSecret).contains("sk-live-super-secret-123"),
                "the ciphertext must not contain the plaintext");

        // Plaintext fields are stored as-is.
        assertEquals("API Key", raw.get("title"));
    }

    @Test
    void describeRawValueReportsByteArrayAsCiphertext() {
        SecureNote note = encryptionService.createNote("Note", "secret-value", "misc");
        Map<String, Object> raw = encryptionService.readRawDocument(note);

        String description = encryptionService.describeRawValue(raw.get("secret_content"));

        assertTrue(description.contains("ciphertext"));
        assertTrue(description.startsWith("byte["));
    }

    @Test
    void countReflectsStoredNotes() {
        assertEquals(0, encryptionService.count());
        encryptionService.createNote("A", "secret-a", "cat");
        encryptionService.createNote("B", "secret-b", "cat");
        assertEquals(2, encryptionService.count());
    }

    @Test
    void deleteAllClearsAllNotes() {
        encryptionService.createNote("A", "secret-a", "cat");
        encryptionService.deleteAll();
        assertEquals(0, encryptionService.count());
    }
}
