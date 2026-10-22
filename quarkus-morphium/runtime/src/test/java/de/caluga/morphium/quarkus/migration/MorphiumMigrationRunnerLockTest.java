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

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * MOR-063: how {@code acquireLock()} classifies a failed lock write. A held lock is recognised by {@link MorphiumDriverException#isDuplicateKey()}, not by the
 * wording of the message, and a write concern error (the lock IS written) is an acquired lock, not a failed run that leaves the lock held for its TTL.
 */
@DisplayName("MorphiumMigrationRunner — classification of a failed lock write (MOR-063)")
class MorphiumMigrationRunnerLockTest {

    private Query<MorphiumMigrationLock> query;
    private MorphiumMigrationRunner runner;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        Morphium morphium = mock(Morphium.class);
        query = mock(Query.class, Answers.RETURNS_DEEP_STUBS);
        when(morphium.createQueryFor(MorphiumMigrationLock.class)).thenReturn(query);

        MorphiumMigrationConfig config = mock(MorphiumMigrationConfig.class);
        when(config.lockCollection()).thenReturn("lock");
        when(config.changeLogCollection()).thenReturn("log");
        when(config.lockTtlSeconds()).thenReturn(30);
        when(config.lockWaitSeconds()).thenReturn(0);
        runner = new MorphiumMigrationRunner(morphium, config);
    }

    private void lockWriteFails(RuntimeException e) {
        when(query.set(anyMap(), anyBoolean(), anyBoolean())).thenThrow(e);
    }

    @Test
    @DisplayName("a duplicate key code is a held lock, whatever the message says")
    void duplicateKeyCodeIsAHeldLock() {
        MorphiumDriverException e = new MorphiumDriverException("the server said no");
        e.setMongoCode(11000);
        lockWriteFails(e);

        assertThatThrownBy(() -> runner.acquireLock()).isNotSameAs(e).hasMessageContaining("Migration lock is");
    }

    @Test
    @DisplayName("a duplicate key in the write errors is a held lock")
    void duplicateKeyInWriteErrorsIsAHeldLock() {
        MorphiumDriverException e = new MorphiumDriverException("the server said no");
        e.setWriteErrors(List.of(Map.of("index", 0, "code", 11000, "errmsg", "E11000")));
        lockWriteFails(e);

        assertThatThrownBy(() -> runner.acquireLock()).isNotSameAs(e).hasMessageContaining("Migration lock is");
    }

    @Test
    @DisplayName("a message that only sounds like a duplicate key is not a held lock")
    void aMessageIsNotEnough() {
        MorphiumDriverException e = new MorphiumDriverException("looks like a duplicate key, but the code is another");
        e.setMongoCode(50);
        lockWriteFails(e);

        assertThatThrownBy(() -> runner.acquireLock()).isSameAs(e);
    }

    @Test
    @DisplayName("any other failure of the write is thrown as it is")
    void otherFailuresAreThrown() {
        RuntimeException e = new IllegalStateException("connection lost");
        lockWriteFails(e);

        assertThatThrownBy(() -> runner.acquireLock()).isSameAs(e);
    }

    @Test
    @DisplayName("a write concern error means the lock is written: the lock is acquired")
    void writeConcernErrorIsAnAcquiredLock() {
        lockWriteFails(new WriteConcernNotSatisfiedException("The write WAS APPLIED but did not satisfy the write concern"));

        assertThatCode(() -> runner.acquireLock()).doesNotThrowAnyException();
    }
}
