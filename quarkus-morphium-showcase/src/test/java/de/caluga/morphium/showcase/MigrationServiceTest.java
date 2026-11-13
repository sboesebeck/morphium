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

import de.caluga.morphium.quarkus.migration.MorphiumMigrationEntry;
import de.caluga.morphium.showcase.migration.MigrationService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the migration-runner showcase feature: seeding creates documents missing the
 * {@code archived} field, running the migration backfills it exactly once, and the run is
 * recorded in the changelog with state {@code EXECUTED}.
 */
@QuarkusTest
class MigrationServiceTest {

    @Inject
    MigrationService migrationService;

    @BeforeEach
    void setUp() {
        migrationService.reset();
    }

    @Test
    void seedCreatesNotesMissingArchivedField() {
        migrationService.seedIfEmpty();

        assertEquals(5, migrationService.noteCount());
        assertEquals(5, migrationService.unmigratedCount());
    }

    @Test
    void runMigrationsNowBackfillsArchivedFlag() {
        migrationService.seedIfEmpty();
        assertEquals(5, migrationService.unmigratedCount());

        migrationService.runMigrationsNow();

        assertEquals(0, migrationService.unmigratedCount());
        assertEquals(5, migrationService.noteCount());
    }

    @Test
    void runMigrationsNowIsIdempotent() {
        migrationService.seedIfEmpty();
        migrationService.runMigrationsNow();
        assertEquals(0, migrationService.unmigratedCount());

        // Running again must not fail and must not change anything -- the changelog already
        // records this change unit's id as EXECUTED, so the runner skips it.
        migrationService.runMigrationsNow();

        assertEquals(0, migrationService.unmigratedCount());
        assertEquals(5, migrationService.noteCount());
    }

    @Test
    void runRecordsAnExecutedChangelogEntry() {
        migrationService.seedIfEmpty();
        migrationService.runMigrationsNow();

        List<MorphiumMigrationEntry> entries = migrationService.findChangelogEntries();

        assertEquals(1, entries.size());
        MorphiumMigrationEntry entry = entries.get(0);
        assertEquals("001-add-archived-flag", entry.getChangeId());
        assertEquals(MorphiumMigrationEntry.ChangeState.EXECUTED, entry.getState());
        assertNotNull(entry.getExecutedAt());
    }

    @Test
    void resetClearsNotesAndChangelog() {
        migrationService.seedIfEmpty();
        migrationService.runMigrationsNow();

        migrationService.reset();

        assertEquals(0, migrationService.noteCount());
        assertTrue(migrationService.findChangelogEntries().isEmpty());
    }

    @Test
    void lockIsNotHeldAfterARunCompletes() {
        migrationService.seedIfEmpty();
        migrationService.runMigrationsNow();

        assertFalse(migrationService.isLockHeld());
    }
}
