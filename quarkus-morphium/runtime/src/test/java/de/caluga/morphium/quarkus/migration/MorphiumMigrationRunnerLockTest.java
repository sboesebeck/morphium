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
package de.caluga.morphium.quarkus.migration;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-063: how the outcome of the lock write is classified. Since MOR-059 a lost upsert on the lock document ({@code _id} exists, {@code expires_at} not reached) raises a
 * {@link MorphiumDriverException} instead of coming back as an error entry in the reply; a write that was applied but missed its write concern raises a
 * {@link WriteConcernNotSatisfiedException}. The lock must be recognised as held by the exception's duplicate key, not by the text of its message.
 */
@DisplayName("MorphiumMigrationRunner — classification of the lock write (MOR-063)")
class MorphiumMigrationRunnerLockTest {

    private static MorphiumDriverException duplicateKey() {
        MorphiumDriverException e = new MorphiumDriverException("some text that does not mention the key problem at all");
        e.setMongoCode(11000);
        return e;
    }

    @Test
    @DisplayName("a reply with a matched document means acquired")
    void matchedReply_isAcquired() {
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> Map.of("n", 1, "nModified", 1))).isTrue();
    }

    @Test
    @DisplayName("a reply with an upserted document means acquired")
    void upsertedReply_isAcquired() {
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> Map.of("n", 1, "upserted", List.of(Map.of("_id", "migration_lock"))))).isTrue();
    }

    @Test
    @DisplayName("a reply without match and without upsert means held (the raise switch is off)")
    void noMatchReply_isHeld() {
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> Map.of("n", 0))).isFalse();
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> null)).isFalse();
    }

    @Test
    @DisplayName("a duplicate key means held, whatever the message says")
    void duplicateKey_isHeld() {
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> {
            throw duplicateKey();
        })).isFalse();
    }

    @Test
    @DisplayName("a duplicate key that is only in the write errors means held")
    void duplicateKeyInTheWriteErrors_isHeld() {
        MorphiumDriverException e = new MorphiumDriverException("failed");
        e.setWriteErrors(List.of(Map.of("code", 11000, "errmsg", "dup")));
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> {
            throw e;
        })).isFalse();
    }

    @Test
    @DisplayName("a write concern error means acquired: the lock IS written, aborting the run would leave it held until it expires")
    void writeConcernError_isAcquired() {
        assertThat(MorphiumMigrationRunner.lockWasAcquired(() -> {
            throw new WriteConcernNotSatisfiedException("The write to lock WAS APPLIED (matched=1, modified=1) but the write concern was not satisfied");
        })).isTrue();
    }

    @Test
    @DisplayName("any other failure is rethrown, also one whose message happens to say duplicate key")
    void otherFailures_areRethrown() {
        assertThatThrownBy(() -> MorphiumMigrationRunner.lockWasAcquired(() -> {
            throw new RuntimeException("Could not index: duplicate key in a config value");
        })).isInstanceOf(RuntimeException.class).hasMessageContaining("config value");

        MorphiumDriverException other = new MorphiumDriverException("path below a scalar");
        other.setMongoCode(28);
        assertThatThrownBy(() -> MorphiumMigrationRunner.lockWasAcquired(() -> {
            throw other;
        })).isSameAs(other);
    }
}
