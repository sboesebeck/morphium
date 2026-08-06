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

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

    // -- Regression: lock TTL renewal (merge blocker #6) --
    //
    // Why this does NOT use the seemingly obvious "a concurrent contender's acquireLock() must
    // fail" approach: InMemoryDriver's upsert path, when its filter (owner-agnostic, matching
    // only an expired/absent lock) matches zero documents, seeds a replacement from the
    // equality predicates (just _id, correctly mirroring real MongoDB) and routes it through
    // storeInternal(). storeInternal() treats an already-existing _id there as a plain replace
    // (remove + insert) rather than raising a duplicate-key error -- unlike its own
    // insertInternal() path, which does implement that check correctly, but which the upsert
    // never reaches. Consequently any contender can steal a still-valid, still-renewed lock
    // under InMemoryDriver regardless of renewal, making acquireLock() non-atomic there (though
    // correctly atomic against a real MongoDB server). A contender-based test is therefore not
    // just flaky but structurally unable to prove anything on this driver.
    //
    // These two tests instead prove renewal directly and positively: while the real migration
    // workload runs on a background thread, the main test thread observes the lock document
    // (read straight from config.lockCollection() via MorphiumMigrationRunner.getLockId(), since
    // releaseLock() deletes it once the run finishes) at two defined points in time DURING the
    // run, and asserts that (1) the owner is unchanged -- no takeover -- and (2) expires_at has
    // moved strictly forward between the two measurements, which is only possible if
    // renewLock() (directly, or via the in-flight heartbeat) actually executed in between.
    //
    //   (a) renewLockBetweenChangeUnitsAdvancesExpiry: measures once during the first change
    //       unit and once during the second, straddling the boundary between them, with a TTL
    //       long enough that the in-flight heartbeat's tick interval exceeds either unit's
    //       sleep -- so only the between-units renewLock() call in the execute() loop can move
    //       expires_at here.
    //
    //   (b) inFlightHeartbeatAdvancesExpiryDuringSingleUnit: both measurements are taken WHILE a
    //       single, long-running change unit is still executing -- there is no "between units"
    //       boundary at all until that one unit returns, so only the in-flight heartbeat started
    //       inside executeMigration() can be responsible for any forward movement observed here.
    //
    // Both use generous (hundreds of ms) margins around every measurement/renewal boundary to
    // avoid flaky timing races while keeping total runtime in the low single-digit seconds.

    /** Reads the single migration-lock document directly, or {@code null} if not currently held. */
    private MorphiumMigrationLock readLockDocument() {
        Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(LOCK_COLLECTION);
        q.f("_id").eq(MorphiumMigrationRunner.getLockId());
        return q.get();
    }

    @Test
    @Order(7)
    @DisplayName("renewLock() between change units advances expires_at without changing the owner")
    void renewLockBetweenChangeUnitsAdvancesExpiry() throws Exception {
        morphium.dropCollection(MorphiumMigrationEntry.class, CHANGELOG_COLLECTION, null);
        morphium.dropCollection(MorphiumMigrationLock.class, LOCK_COLLECTION, null);

        // Long (10s) TTL relative to the unit sleeps below: the in-flight heartbeat's tick
        // interval (~TTL/3, here ~3.3s, floored at 200ms) is far longer than either unit's
        // 600ms sleep, so it cannot fire during either one. The only thing that can move
        // expires_at forward between this test's two measurements is the renewLock() call
        // between units.
        var config = new TestMigrationConfig() {
            @Override public int lockTtlSeconds() { return 10; }
        };
        var slowRunner = new MorphiumMigrationRunner(morphium, config);

        SlowMigration.SLEEP_MS = 600L;
        SlowMigration2.SLEEP_MS = 600L;
        try {
            AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    slowRunner.execute(List.of(SlowMigration.class.getName(), SlowMigration2.class.getName(),
                            AddCategoryMigration.class.getName()));
                } catch (Throwable t) {
                    workerFailure.set(t);
                }
            });
            worker.start();

            // t=300ms: comfortably inside the first unit (600ms total), well before it returns
            // and therefore well before the renewLock() call that only happens once it does.
            Thread.sleep(300L);
            MorphiumMigrationLock during1 = readLockDocument();
            assertThat(during1).as("lock document must exist while migrations are running").isNotNull();

            // t=900ms: 300ms into the second unit (which started at ~600ms) -- comfortably
            // AFTER the renewLock() call that ran between the two units (~600ms) and
            // comfortably BEFORE the second unit itself finishes (~1200ms).
            Thread.sleep(600L);
            MorphiumMigrationLock during2 = readLockDocument();
            assertThat(during2).as("lock document must still exist while migrations are running").isNotNull();

            worker.join(5000L);
            assertThat(worker.isAlive()).as("migration worker thread should have finished").isFalse();
            assertThat(workerFailure.get()).as("migration run must have completed without error").isNull();

            assertThat(during2.getOwner())
                    .as("owner must be unchanged between the two measurements -- no takeover happened")
                    .isEqualTo(during1.getOwner());
            assertThat(during2.getExpiresAt())
                    .as("expires_at must have been pushed forward by the between-units renewLock() call")
                    .isAfter(during1.getExpiresAt());

            Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
            q.setCollectionName(CHANGELOG_COLLECTION);
            q.f("_id").eq("002-add-category");
            assertThat(q.get()).isNotNull();

            // Lock released at the end of a successful run.
            Query<?> lockQ = morphium.createQueryFor(MorphiumMigrationLock.class);
            lockQ.setCollectionName(LOCK_COLLECTION);
            assertThat(lockQ.countAll()).isZero();
        } finally {
            SlowMigration.SLEEP_MS = 1500L;
            SlowMigration2.SLEEP_MS = 1500L;
        }
    }

    @Test
    @Order(8)
    @DisplayName("In-flight lock heartbeat advances expires_at during a single long-running change unit")
    void inFlightHeartbeatAdvancesExpiryDuringSingleUnit() throws Exception {
        morphium.dropCollection(MorphiumMigrationEntry.class, CHANGELOG_COLLECTION, null);
        morphium.dropCollection(MorphiumMigrationLock.class, LOCK_COLLECTION, null);

        // Short (1s) TTL: the in-flight heartbeat's tick interval (~333ms with this TTL) is far
        // shorter than the single unit's 2s sleep below, so it ticks several times while that
        // one unit is still running. Both measurements are taken WHILE this single unit is
        // executing, so renewLock() in the execute() loop cannot be responsible for anything
        // observed here -- there is no "between units" until this one unit returns.
        var config = new TestMigrationConfig() {
            @Override public int lockTtlSeconds() { return 1; }
        };
        var slowRunner = new MorphiumMigrationRunner(morphium, config);

        SlowMigration2.SLEEP_MS = 2000L;
        try {
            AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    slowRunner.execute(List.of(SlowMigration2.class.getName(), AddCategoryMigration.class.getName()));
                } catch (Throwable t) {
                    workerFailure.set(t);
                }
            });
            worker.start();

            // t=600ms: well after the heartbeat's first tick (fires ~333ms after the unit
            // starts, given the 1s TTL and its floor-adjusted ~333ms interval), well before the
            // unit itself finishes at ~2000ms.
            Thread.sleep(600L);
            MorphiumMigrationLock during1 = readLockDocument();
            assertThat(during1).as("lock document must exist while the unit is still running").isNotNull();

            // t=1600ms: a full second later -- several more heartbeat ticks have had the chance
            // to fire in between (~333ms interval), still comfortably before the unit finishes
            // (~2000ms).
            Thread.sleep(1000L);
            MorphiumMigrationLock during2 = readLockDocument();
            assertThat(during2).as("lock document must still exist while the unit is still running").isNotNull();

            worker.join(5000L);
            assertThat(worker.isAlive()).as("migration worker thread should have finished").isFalse();
            assertThat(workerFailure.get()).as("migration run must have completed without error").isNull();

            assertThat(during2.getOwner())
                    .as("owner must be unchanged between the two measurements -- no takeover happened")
                    .isEqualTo(during1.getOwner());
            assertThat(during2.getExpiresAt())
                    .as("expires_at must have been pushed forward by the in-flight heartbeat while the unit "
                            + "was still executing")
                    .isAfter(during1.getExpiresAt());

            Query<MorphiumMigrationEntry> q = morphium.createQueryFor(MorphiumMigrationEntry.class);
            q.setCollectionName(CHANGELOG_COLLECTION);
            q.f("_id").eq("002-add-category");
            assertThat(q.get()).isNotNull();

            Query<?> lockQ = morphium.createQueryFor(MorphiumMigrationLock.class);
            lockQ.setCollectionName(LOCK_COLLECTION);
            assertThat(lockQ.countAll()).isZero();
        } finally {
            SlowMigration2.SLEEP_MS = 1500L;
        }
    }

    @Test
    @Order(9)
    @DisplayName("acquireLockWithWait: waits for a held lock instead of failing immediately")
    void acquireLockWaitsForHeldLock() throws Exception {
        morphium.dropCollection(MorphiumMigrationLock.class, LOCK_COLLECTION, null);

        // Manually hold the lock, simulating another instance already running migrations.
        // Uses MorphiumMigrationRunner.getLockId() -- the real lock-document id -- rather than
        // a copy-pasted string literal, so that renaming MorphiumMigrationRunner.LOCK_ID cannot
        // silently make this test blind to its own bugs by upserting a different (unrelated)
        // lock document than the one acquireLock() actually reads and writes.
        MorphiumMigrationLock heldLock = new MorphiumMigrationLock();
        heldLock.setId(MorphiumMigrationRunner.getLockId());
        heldLock.setOwner("other-instance");
        heldLock.setAcquiredAt(new Date());
        heldLock.setExpiresAt(new Date(System.currentTimeMillis() + 5000L));
        morphium.store(heldLock, LOCK_COLLECTION, null);

        // Release it from a background thread after a short delay, simulating the other
        // instance finishing its migration run.
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
                return;
            }
            Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
            q.setCollectionName(LOCK_COLLECTION);
            q.f("_id").eq(MorphiumMigrationRunner.getLockId());
            morphium.delete(q);
        });
        releaser.start();

        var waitingConfig = new TestMigrationConfig() {
            @Override public int lockWaitSeconds() { return 5; }
        };
        var waitingRunner = new MorphiumMigrationRunner(morphium, waitingConfig);

        // Must NOT throw: waits past the releaser's delete, then successfully acquires the
        // lock. Uses a real migration (not an empty list) -- execute() with an empty list
        // returns before ever calling acquireLockWithWait(), which would make this test
        // pass trivially without exercising the wait logic at all.
        waitingRunner.execute(List.of(AddCategoryMigration.class.getName()));
        releaser.join();
    }

    // ------------------------------------------------------------------
    // Test config with isolated collection names
    // ------------------------------------------------------------------

    private static class TestMigrationConfig implements MorphiumMigrationConfig {
        @Override public boolean migrateAtStart() { return true; }
        @Override public String changeLogCollection() { return CHANGELOG_COLLECTION; }
        @Override public String lockCollection() { return LOCK_COLLECTION; }
        @Override public int lockTtlSeconds() { return 30; }
        @Override public int lockWaitSeconds() { return 0; }
    }
}
