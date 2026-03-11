package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that ObjectMapperImpl uses the context ClassLoader for entity scanning
 * and that clearEntityCache() allows a fresh scan with a new ClassLoader.
 * <p>
 * In environments like Quarkus dev mode, the ClassLoader is replaced on hot-reload.
 * Without context ClassLoader support, Class.forName() fails for classes loaded by
 * the new ClassLoader, and the static entity cache retains stale references.
 * <p>
 * All tests run with InMemoryDriver — no MongoDB required.
 */
@Tag("inmemory")
public class ObjectMapperClassLoaderTest {

    @Entity(collectionName = "classloader_test_entity")
    public static class ClassLoaderTestEntity {
        @Id
        public MorphiumId id;

        public String data;

        public ClassLoaderTestEntity() {}

        public ClassLoaderTestEntity(String data) {
            this.data = data;
        }
    }

    private Morphium morphium;

    @BeforeEach
    public void setup() {
        // Clear any cached entity scan from previous tests
        ObjectMapperImpl.clearEntityCache();
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void clearEntityCache_allowsFreshScan() {
        // First Morphium instance populates the static entity cache
        MorphiumConfig cfg1 = new MorphiumConfig("classloader_test_db_1", 10, 10_000, 1_000);
        cfg1.driverSettings().setDriverName(InMemoryDriver.driverName);
        Morphium m1 = new Morphium(cfg1);

        ClassLoaderTestEntity e1 = new ClassLoaderTestEntity("from-first-instance");
        m1.store(e1);
        assertThat(e1.id).isNotNull();

        m1.close();

        // Clear the cache (simulates what Quarkus extension does before hot-reload)
        ObjectMapperImpl.clearEntityCache();

        // Second Morphium instance should perform a fresh scan
        MorphiumConfig cfg2 = new MorphiumConfig("classloader_test_db_2", 10, 10_000, 1_000);
        cfg2.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg2);

        // Verify the entity type is still recognized after cache clear + fresh scan
        ClassLoaderTestEntity e2 = new ClassLoaderTestEntity("from-second-instance");
        morphium.store(e2);
        assertThat(e2.id).isNotNull();

        ClassLoaderTestEntity loaded = morphium.createQueryFor(ClassLoaderTestEntity.class)
            .f("id").eq(e2.id)
            .get();
        assertThat(loaded).isNotNull();
        assertThat(loaded.data).isEqualTo("from-second-instance");
    }

    @Test
    public void contextClassLoader_isUsedForEntityScanning() {
        // Verify that the current context ClassLoader can find our test entity
        ClassLoader contextCl = Thread.currentThread().getContextClassLoader();
        assertThat(contextCl).isNotNull();

        // Create Morphium — this triggers entity scanning using the context ClassLoader
        MorphiumConfig cfg = new MorphiumConfig("classloader_ctx_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);

        // If context ClassLoader is not used, this entity from the test ClassLoader
        // would not be found and getCollectionName would return null or fail
        String collName = morphium.getMapper().getCollectionName(ClassLoaderTestEntity.class);
        assertThat(collName).isEqualTo("classloader_test_entity");
    }
}
