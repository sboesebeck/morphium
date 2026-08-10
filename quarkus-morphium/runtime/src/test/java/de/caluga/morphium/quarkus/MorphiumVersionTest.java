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
package de.caluga.morphium.quarkus;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code META-INF/morphium-version.properties} is correctly populated by
 * Maven resource filtering at build time, and that {@link MorphiumVersion} reads it back.
 *
 * <p>Regression test: {@code morphium.version} previously referenced an undefined
 * {@code ${morphium.version}} Maven property (no such property exists in the reactor),
 * so filtering left the literal placeholder string in the built JAR instead of the
 * actual version — {@link #morphiumVersion()} would silently report a wrong value.
 * Since this module is lockstep-versioned with Morphium core, the property now reads
 * {@code ${project.version}}, the same expression already used for
 * {@code extension.version}.
 */
class MorphiumVersionTest {

    @Test
    @DisplayName("extensionVersion() is populated, not \"unknown\" and not a literal placeholder")
    void extensionVersionIsResolved() {
        String version = MorphiumVersion.extensionVersion();
        assertThat(version).isNotEqualTo("unknown");
        assertThat(version).doesNotContain("${");
    }

    @Test
    @DisplayName("morphiumVersion() is populated, not \"unknown\" and not a literal placeholder")
    void morphiumVersionIsResolved() {
        String version = MorphiumVersion.morphiumVersion();
        assertThat(version).isNotEqualTo("unknown");
        assertThat(version).doesNotContain("${");
    }

    @Test
    @DisplayName("morphiumVersion() and extensionVersion() are identical (lockstep versioning)")
    void morphiumVersionMatchesExtensionVersion() {
        // Both properties resolve from ${project.version} on this reactor -- lockstep
        // versioning means they must always be the same value.
        assertThat(MorphiumVersion.morphiumVersion())
                .isEqualTo(MorphiumVersion.extensionVersion());
    }

    @Test
    @DisplayName("jakartaDataVersion() is populated, not \"unknown\" and not a literal placeholder")
    void jakartaDataVersionIsResolved() {
        String version = MorphiumVersion.jakartaDataVersion();
        assertThat(version).isNotEqualTo("unknown");
        assertThat(version).doesNotContain("${");
    }
}
