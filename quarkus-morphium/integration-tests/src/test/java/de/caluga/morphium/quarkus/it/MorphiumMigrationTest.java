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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationConfig;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationEntry;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationLock;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationRunner;
import de.caluga.morphium.query.Query;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for the Morphium migration framework.
 * Tests programmatic migration execution using {@link MorphiumMigrationRunner};
 * the migrate-at-start flag in {@code TestMigrationConfig} is not used in this test.
 */
@QuarkusTest
@DisplayName("Morphium Migration Framework")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumMigrationTest {

    @Inject
    Morphium morphium;

    private static final String CHANGELOG_COLLECTION = "testChangeLog";
    private static final String LOCK_COLLECTION = "testMigrationLock";

    private MorphiumMigrationRunner runner;

    @BeforeEach
    void setUp() {
        runner = new MorphiumMigrationRunner(morphium, new TestMigrationConfig());
    }

    @Test
    @Order(1)
    @DisplayName("Migrations execute in order and create changelog entries")
    void migrationsExecuteAndTrack() {
        // Clean up from potential previous runs
        morphium.dropCollection(MorphiumMigrationEntry.class, CHANGELOG_COLLECTION, null);
        morphium.dropCollection(MorphiumMigrationLock.class, LOCK_COLLECTION, null);
        morphium.dropCollection(ItemEntity.class);

        List<String> migrations = List.of(
                InitItemsMigration.class.getName(),
                AddCategoryMigration.class.getName()
        );

        runner.execute(migrations);

        // Verify changelog entries
        Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
        q.setCollectionName(CHANGELOG_COLLECTION);
        q.sort("order");
        List<MorphiumMigrationEntry> entries = q.asList();

        assertThat(entries).hasSize(2);

        assertThat(entries.get(0).getChangeId()).isEqualTo("001-init-items");
        assertThat(entries.get(0).getState()).isEqualTo(MorphiumMigrationEntry.ChangeState.EXECUTED);
        assertThat(entries.get(0).getAuthor()).isEqualTo("test");
        assertThat(entries.get(0).getExecutionTimeMs()).isGreaterThanOrEqualTo(0);

        assertThat(entries.get(1).getChangeId()).isEqualTo("002-add-category");
        assertThat(entries.get(1).getState()).isEqualTo(MorphiumMigrationEntry.ChangeState.EXECUTED);
    }

    @Test
    @Order(2)
    @DisplayName("Migrations actually modify the database")
    void migrationsModifyDatabase() {
        Query<ItemEntity> q = morphium.createQueryFor(ItemEntity.class);
        q.f("tag").in(List.of("migration-v1", "migration-v2"));
        List<ItemEntity> items = q.asList();

        assertThat(items).hasSizeGreaterThanOrEqualTo(2);
        assertThat(items).extracting(ItemEntity::getName)
                .contains("Migrated Widget", "Migrated Gadget");
    }

    @Test
    @Order(3)
    @DisplayName("Already executed migrations are skipped on re-run")
    void alreadyExecutedMigrationsAreSkipped() {
        // Count items before second run
        long countBefore = morphium.createQueryFor(ItemEntity.class)
                .f("tag").in(List.of("migration-v1", "migration-v2"))
                .countAll();

        // Re-run the same migrations
        List<String> migrations = List.of(
                InitItemsMigration.class.getName(),
                AddCategoryMigration.class.getName()
        );
        runner.execute(migrations);

        // Count items after — should be same (no duplicates)
        long countAfter = morphium.createQueryFor(ItemEntity.class)
                .f("tag").in(List.of("migration-v1", "migration-v2"))
                .countAll();

        assertThat(countAfter).isEqualTo(countBefore);

        // Changelog should still have exactly 2 entries
        Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
        q.setCollectionName(CHANGELOG_COLLECTION);
        assertThat(q.countAll()).isEqualTo(2);
    }

    @Test
    @Order(4)
    @DisplayName("Lock is released after migrations complete")
    void lockIsReleasedAfterMigrations() {
        Query<?> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(LOCK_COLLECTION);
        assertThat(q.countAll()).isZero();
    }

    @Test
    @Order(5)
    @DisplayName("Empty migration list is handled gracefully")
    void emptyMigrationList() {
        // Should not throw
        runner.execute(List.of());
    }

    @Test
    @Order(6)
    @DisplayName("Failed migration triggers rollback and records ROLLED_BACK state")
    void failedMigrationTriggersRollback() {
        FailingMigration.rollbackExecuted = false;

        assertThatThrownBy(() -> runner.execute(List.of(FailingMigration.class.getName())))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("999-failing");

        // Verify rollback was executed
        assertThat(FailingMigration.rollbackExecuted).isTrue();

        // Verify changelog entry has ROLLED_BACK state
        Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
        q.setCollectionName(CHANGELOG_COLLECTION);
        q.f("_id").eq("999-failing");
        MorphiumMigrationEntry entry = q.get();

        assertThat(entry).isNotNull();
        assertThat(entry.getState()).isEqualTo(MorphiumMigrationEntry.ChangeState.ROLLED_BACK);

        // Verify lock is released even after failure
        Query<?> lockQ = morphium.createQueryFor(MorphiumMigrationLock.class);
        lockQ.setCollectionName(LOCK_COLLECTION);
        assertThat(lockQ.countAll()).isZero();
    }

    // ------------------------------------------------------------------
    // Test config with isolated collection names
    // ------------------------------------------------------------------

    private static class TestMigrationConfig implements MorphiumMigrationConfig {
        @Override public boolean migrateAtStart() { return true; }
        @Override public String changeLogCollection() { return CHANGELOG_COLLECTION; }
        @Override public String lockCollection() { return LOCK_COLLECTION; }
        @Override public int lockTtlSeconds() { return 30; }
    }
}
