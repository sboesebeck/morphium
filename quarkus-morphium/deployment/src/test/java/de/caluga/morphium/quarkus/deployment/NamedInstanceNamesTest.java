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
package de.caluga.morphium.quarkus.deployment;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-032: the names of the named instances are read from the keys {@code quarkus.morphium.named."name".*} at build time. */
class NamedInstanceNamesTest {

    @Test
    @DisplayName("quoted names, bare names, one name for many keys, in the order of the first key")
    void namesAreReadFromTheKeys() {
        List<String> keys = List.of(
                "quarkus.morphium.database",
                "quarkus.morphium.named.\"analytics\".database",
                "quarkus.morphium.named.\"analytics\".hosts",
                "quarkus.morphium.named.\"audit.log\".database",
                "quarkus.morphium.named.plain.database",
                "quarkus.morphium.named.\"analytics\".cache.global-valid-time");

        assertThat(MorphiumProcessor.namedInstanceNames(keys)).containsExactly("analytics", "audit.log", "plain");
    }

    @Test
    @DisplayName("keys that are not a named instance, and broken keys, give no name")
    void otherKeysAreIgnored() {
        List<String> keys = List.of(
                "quarkus.morphium.hosts",
                "quarkus.morphium.named.",
                "quarkus.morphium.named.\"unclosed.database",
                "quarkus.morphium.named.\"\".database",
                "quarkus.datasource.named.\"x\".database");

        assertThat(MorphiumProcessor.namedInstanceNames(keys)).isEmpty();
    }
}
