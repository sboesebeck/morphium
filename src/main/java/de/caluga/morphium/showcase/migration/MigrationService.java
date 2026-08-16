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
package de.caluga.morphium.showcase.migration;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationConfig;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationEntry;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationLock;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationRunner;
import de.caluga.morphium.showcase.migration.entity.MigrationNote;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

/**
 * Seeds demo data and drives a manual, on-demand run of the migration runner for this
 * showcase feature.
 *
 * <p>{@code quarkus.morphium.migration.migrate-at-start} only runs pending migrations
 * <em>once</em> at application startup &mdash; there is no built-in "run again on demand"
 * button. To let visitors trigger a run whenever they like, this service constructs its own
 * {@link MorphiumMigrationRunner} with a fixed {@link MorphiumMigrationConfig} and calls
 * {@code execute(...)} directly, exactly like the Quarkus extension's own
 * {@code MorphiumRecorder} does internally at startup.</p>
 */
@ApplicationScoped
public class MigrationService {

    /**
     * Fully qualified class name of the single demo migration. In a real application this
     * list would come from the build-time Jandex scan the extension performs automatically;
     * this showcase has exactly one migration, so the list is a one-element constant.
     */
    private static final List<String> CHANGE_UNIT_CLASS_NAMES =
            List.of(AddArchivedFlagMigration.class.getName());

    @Inject
    Morphium morphium;

    /**
     * Fixed configuration mirroring this showcase's {@code application.properties} defaults
     * for the {@code quarkus.morphium.migration.*} properties (all left at their built-in
     * defaults here, since the CDI-managed {@code MorphiumMigrationConfig} is not directly
     * constructible outside of Quarkus's config-mapping machinery).
     */
    private static final MorphiumMigrationConfig DEMO_CONFIG = new MorphiumMigrationConfig() {
        @Override
        public boolean migrateAtStart() {
            return false;
        }

        @Override
        public String changeLogCollection() {
            return "morphiumChangeLog";
        }

        @Override
        public String lockCollection() {
            return "morphiumMigrationLock";
        }

        @Override
        public int lockTtlSeconds() {
            return 60;
        }

        @Override
        public int lockWaitSeconds() {
            return 0;
        }
    };

    /** Seeds five {@link MigrationNote} documents without the {@code archived} field, if empty. */
    public void seedIfEmpty() {
        if (morphium.createQueryFor(MigrationNote.class).countAll() == 0) {
            for (int i = 1; i <= 5; i++) {
                morphium.store(MigrationNote.builder()
                        .title("Note " + i + " (pre-migration schema)")
                        .archived(null)
                        .build());
            }
        }
    }

    /** Runs the demo migration now. Idempotent -- safe to call repeatedly. */
    public void runMigrationsNow() {
        new MorphiumMigrationRunner(morphium, DEMO_CONFIG).execute(CHANGE_UNIT_CLASS_NAMES);
    }

    public long noteCount() {
        return morphium.createQueryFor(MigrationNote.class).countAll();
    }

    public long unmigratedCount() {
        return morphium.createQueryFor(MigrationNote.class)
                .f(MigrationNote.Fields.archived).eq(null).countAll();
    }

    public List<MigrationNote> findAllNotes() {
        return morphium.createQueryFor(MigrationNote.class)
                .sort(java.util.Map.of(MigrationNote.Fields.createdAt, 1))
                .asList();
    }

    /** All changelog entries, newest first. */
    public List<MorphiumMigrationEntry> findChangelogEntries() {
        return morphium.createQueryFor(MorphiumMigrationEntry.class)
                .setCollectionName(DEMO_CONFIG.changeLogCollection())
                .sort(java.util.Map.of("executedAt", -1))
                .asList();
    }

    /** Whether the migration lock is currently held (a run is in progress on some instance). */
    public boolean isLockHeld() {
        return morphium.createQueryFor(MorphiumMigrationLock.class)
                .setCollectionName(DEMO_CONFIG.lockCollection())
                .countAll() > 0;
    }

    /** Resets this demo: drops both the notes and the changelog, so the migration can be re-observed. */
    public void reset() {
        morphium.dropCollection(MigrationNote.class);
        morphium.createQueryFor(MorphiumMigrationEntry.class)
                .setCollectionName(DEMO_CONFIG.changeLogCollection())
                .delete();
    }
}
