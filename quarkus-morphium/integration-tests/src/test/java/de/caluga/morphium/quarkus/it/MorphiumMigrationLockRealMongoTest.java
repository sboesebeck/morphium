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
import de.caluga.morphium.quarkus.testing.MongoMorphiumTestProfile;
import de.caluga.morphium.query.Query;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-063 against a real MongoDB (Dev Services, or MORPHIUM_TEST_HOSTS): a lock that is held is reported as held, by the duplicate key of the lost upsert of
 * the second runner, and the first runner is not disturbed.
 */
@QuarkusTest
@ExtendWith(DockerAvailableCondition.class)
@TestProfile(MongoMorphiumTestProfile.class)
@DisplayName("Morphium migration lock on a real MongoDB")
class MorphiumMigrationLockRealMongoTest {

    private static final String CHANGELOG_COLLECTION = "mor063ChangeLog";
    private static final String LOCK_COLLECTION = "mor063Lock";

    @Inject
    Morphium morphium;

    private final MorphiumMigrationConfig config = new MorphiumMigrationConfig() {
        @Override public boolean migrateAtStart() { return true; }
        @Override public String changeLogCollection() { return CHANGELOG_COLLECTION; }
        @Override public String lockCollection() { return LOCK_COLLECTION; }
        @Override public int lockTtlSeconds() { return 30; }
        @Override public int lockWaitSeconds() { return 0; }
    };

    @BeforeEach
    void clean() {
        morphium.dropCollection(MorphiumMigrationEntry.class, CHANGELOG_COLLECTION, null);
        morphium.dropCollection(MorphiumMigrationLock.class, LOCK_COLLECTION, null);
    }

    private MorphiumMigrationLock readLock() {
        Query<MorphiumMigrationLock> q = morphium.createQueryFor(MorphiumMigrationLock.class);
        q.setCollectionName(LOCK_COLLECTION);
        q.f("_id").eq(MorphiumMigrationRunner.getLockId());
        return q.get();
    }

    @Test
    @DisplayName("a lock of another instance that has not expired is reported as held")
    void heldLockIsReported() {
        MorphiumMigrationLock held = new MorphiumMigrationLock();
        held.setId(MorphiumMigrationRunner.getLockId());
        held.setOwner("other-instance");
        held.setAcquiredAt(new Date());
        held.setExpiresAt(new Date(System.currentTimeMillis() + 60_000L));
        morphium.store(held, LOCK_COLLECTION, null);

        assertThatThrownBy(() -> new MorphiumMigrationRunner(morphium, config).execute(List.of(AddCategoryMigration.class.getName())))
                .hasMessageContaining("Migration lock is held by 'other-instance'");

        assertThat(readLock().getOwner()).as("the lock of the other instance is untouched").isEqualTo("other-instance");
    }

    @Test
    @DisplayName("an expired lock is taken over")
    void expiredLockIsTakenOver() {
        MorphiumMigrationLock expired = new MorphiumMigrationLock();
        expired.setId(MorphiumMigrationRunner.getLockId());
        expired.setOwner("crashed-instance");
        expired.setAcquiredAt(new Date(System.currentTimeMillis() - 120_000L));
        expired.setExpiresAt(new Date(System.currentTimeMillis() - 60_000L));
        morphium.store(expired, LOCK_COLLECTION, null);

        new MorphiumMigrationRunner(morphium, config).execute(List.of(AddCategoryMigration.class.getName()));

        assertThat(readLock()).as("the lock is released at the end of the run").isNull();
    }

    @Test
    @DisplayName("two runners: the second one is told that the lock is held while the first one runs on")
    void twoRunnersOnlyOneRuns() throws Exception {
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        Thread first = new Thread(() -> {
            try {
                new MorphiumMigrationRunner(morphium, config).execute(List.of(SlowMigration.class.getName()));
            } catch (Throwable t) {
                firstFailure.set(t);
            }
        });
        first.start();

        long deadline = System.currentTimeMillis() + 10_000L;
        while (readLock() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertThat(readLock()).as("the first runner holds the lock").isNotNull();

        assertThatThrownBy(() -> new MorphiumMigrationRunner(morphium, config).execute(List.of(SlowMigration.class.getName())))
                .hasMessageContaining("Migration lock is held by");

        first.join(30_000L);
        assertThat(firstFailure.get()).as("the first runner finished without a failure").isNull();
        assertThat(readLock()).as("the lock is released at the end of the run").isNull();
    }
}
