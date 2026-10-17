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
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-063: two runners contend for the lock on the InMemoryDriver and, if {@code -Dmorphium.uri=mongodb://host:port/db} is set, on a real MongoDB. The second runner finds
 * the lock held (the lost upsert raises a duplicate key since MOR-059), and takes it over after the TTL.
 */
@DisplayName("MorphiumMigrationRunner — two runners contend for the lock (MOR-063)")
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class MorphiumMigrationRunnerLockContentionTest {

    private static final String URI_PROPERTY = "morphium.uri";

    private Morphium morphium;
    private MorphiumMigrationRunner first;
    private MorphiumMigrationRunner second;

    private static MorphiumMigrationConfig config(String lockCollection, int ttlSeconds) {
        return new MorphiumMigrationConfig() {
            public boolean migrateAtStart() {
                return false;
            }

            public String changeLogCollection() {
                return "mor063_changelog";
            }

            public String lockCollection() {
                return lockCollection;
            }

            public int lockTtlSeconds() {
                return ttlSeconds;
            }

            public int lockWaitSeconds() {
                return 0;
            }
        };
    }

    private void start(String driver) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("mor063_" + UUID.randomUUID().toString().substring(0, 8));

        if (driver.equals("inmem")) {
            cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        } else {
            String uri = System.getProperty(URI_PROPERTY);
            assumeTrue(uri != null && !uri.isBlank(), "-D" + URI_PROPERTY + " is not set: no real MongoDB to test against");
            URI u = URI.create(uri);
            cfg.driverSettings().setDriverName(PooledDriver.driverName);
            cfg.clusterSettings().addHostToSeed(u.getHost(), u.getPort() > 0 ? u.getPort() : 27017);
        }

        morphium = new Morphium(cfg);
        String lockCollection = "mor063_lock";
        first = new MorphiumMigrationRunner(morphium, config(lockCollection, 1));
        second = new MorphiumMigrationRunner(morphium, config(lockCollection, 1));
    }

    @AfterEach
    void stop() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"inmem", "mongodb"})
    @DisplayName("the second runner finds a held lock; after the TTL it takes the lock over")
    void secondRunnerFindsTheLockHeldAndTakesItOverAfterTheTtl(String driver) throws Exception {
        start(driver);
        first.acquireLock();

        assertThatThrownBy(second::acquireLock)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Migration lock is held by");

        Thread.sleep(1300); // lockTtlSeconds = 1
        second.acquireLock(); // expired: the update matches, no duplicate key
        assertThatThrownBy(first::acquireLock).hasMessageContaining("Migration lock is held by");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"inmem", "mongodb"})
    @DisplayName("exactly one of several concurrent contenders gets the lock")
    void exactlyOneContenderGetsTheLock(String driver) throws Exception {
        start(driver);
        int contenders = 6;
        MorphiumMigrationRunner[] runners = new MorphiumMigrationRunner[contenders];

        for (int i = 0; i < contenders; i++) {
            runners[i] = new MorphiumMigrationRunner(morphium, config("mor063_lock_race", 30));
        }

        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger winners = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger held = new java.util.concurrent.atomic.AtomicInteger();
        Thread[] threads = new Thread[contenders];

        for (int i = 0; i < contenders; i++) {
            MorphiumMigrationRunner r = runners[i];
            threads[i] = new Thread(() -> {
                try {
                    go.await();
                    r.acquireLock();
                    winners.incrementAndGet();
                } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("Migration lock is held by")) {
                        held.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            threads[i].start();
        }

        go.countDown();

        for (Thread t : threads) {
            t.join(30_000);
        }

        assertThat(winners.get()).as("exactly one winner").isEqualTo(1);
        assertThat(held.get()).as("all others see the lock held").isEqualTo(contenders - 1);
    }
}
