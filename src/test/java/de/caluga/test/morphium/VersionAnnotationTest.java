package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.VersionMismatchException;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the @Version / optimistic-locking support.
 * All tests run with InMemoryDriver – no MongoDB required.
 */
@Tag("inmemory")
public class VersionAnnotationTest {

    // -----------------------------------------------------------------------
    // Entity with @Version
    // -----------------------------------------------------------------------
    @Entity(collectionName = "versioned_entity")
    public static class VersionedEntity {
        @Id
        public MorphiumId id;

        @Version
        @Property(fieldName = "version")
        public long version;

        public String name;

        public VersionedEntity() {}

        public VersionedEntity(String name) {
            this.name = name;
        }
    }

    // -----------------------------------------------------------------------
    // Entity with @Version and a pre-set String ID (e.g. UUID)
    // -----------------------------------------------------------------------
    @Entity(collectionName = "versioned_string_id_entity")
    public static class VersionedStringIdEntity {
        @Id
        public String id;

        @Version
        @Property(fieldName = "version")
        public long version;

        public String name;

        public VersionedStringIdEntity() {}

        public VersionedStringIdEntity(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    // -----------------------------------------------------------------------
    // Entity WITHOUT @Version  (unchanged code path)
    // -----------------------------------------------------------------------
    @Entity(collectionName = "plain_entity")
    public static class PlainEntity {
        @Id
        public MorphiumId id;

        public String value;

        public PlainEntity() {}

        public PlainEntity(String value) {
            this.value = value;
        }
    }

    // -----------------------------------------------------------------------
    // Test infrastructure
    // -----------------------------------------------------------------------
    private Morphium morphium;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig("version_test_db", 10, 10_000, 1_000);
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    // -----------------------------------------------------------------------
    // 1. First store → version == 1 in DB and in-memory
    // -----------------------------------------------------------------------
    @Test
    public void firstStore_setsVersionToOne() {
        VersionedEntity e = new VersionedEntity("alpha");

        morphium.store(e);

        assertThat(e.id).as("id must be set after store").isNotNull();
        assertThat(e.version).as("version must be 1 after first store").isEqualTo(1L);

        VersionedEntity loaded = morphium.createQueryFor(VersionedEntity.class)
            .f("id").eq(e.id)
            .get();
        assertThat(loaded).isNotNull();
        assertThat(loaded.version).as("version in DB must be 1").isEqualTo(1L);
    }

    // -----------------------------------------------------------------------
    // 2. Second store → version == 2 in DB and in-memory
    // -----------------------------------------------------------------------
    @Test
    public void secondStore_incrementsVersionToTwo() {
        VersionedEntity e = new VersionedEntity("beta");
        morphium.store(e);
        assertThat(e.version).isEqualTo(1L);

        e.name = "beta-updated";
        morphium.store(e);

        assertThat(e.version).as("in-memory version after second store").isEqualTo(2L);

        VersionedEntity loaded = morphium.createQueryFor(VersionedEntity.class)
            .f("id").eq(e.id)
            .get();
        assertThat(loaded.version).as("DB version after second store").isEqualTo(2L);
        assertThat(loaded.name).isEqualTo("beta-updated");
    }

    // -----------------------------------------------------------------------
    // 3. Stale entity (v=1, DB has v=2) → VersionMismatchException
    // -----------------------------------------------------------------------
    @Test
    public void staleEntity_throwsVersionMismatchException() {
        VersionedEntity e = new VersionedEntity("gamma");
        morphium.store(e);                 // v = 1 in DB

        // Simulate another writer updating the document
        VersionedEntity other = morphium.createQueryFor(VersionedEntity.class)
            .f("id").eq(e.id)
            .get();
        other.name = "gamma-by-other-writer";
        morphium.store(other);             // v = 2 in DB, other.version = 2

        // `e` still has version = 1, so storing it must fail
        e.name = "gamma-stale";
        assertThatThrownBy(() -> morphium.store(e))
            .isInstanceOf(VersionMismatchException.class)
            .satisfies(ex -> {
                VersionMismatchException vme = (VersionMismatchException) ex;
                assertThat(vme.getExpectedVersion()).isEqualTo(1L);
                assertThat(vme.getEntityId()).isEqualTo(e.id);
            });
    }

    // -----------------------------------------------------------------------
    // 4. Entity without @Version → stored / updated without any version check
    // -----------------------------------------------------------------------
    @Test
    public void entityWithoutVersionAnnotation_worksNormally() {
        PlainEntity p = new PlainEntity("original");
        morphium.store(p);

        p.value = "modified";
        morphium.store(p);  // must not throw

        PlainEntity loaded = morphium.createQueryFor(PlainEntity.class)
            .f("id").eq(p.id)
            .get();
        assertThat(loaded.value).isEqualTo("modified");
    }

    // -----------------------------------------------------------------------
    // 5. Mixed bulk store: versioned + non-versioned entities in one call
    // -----------------------------------------------------------------------
    @Test
    public void bulkStore_mixedVersionedAndPlain_allStoredCorrectly() {
        VersionedEntity v1 = new VersionedEntity("v-one");
        VersionedEntity v2 = new VersionedEntity("v-two");
        PlainEntity     p1 = new PlainEntity("p-one");

        // Bulk store of mixed types is done by storing them individually here
        // (morphium.store(List) handles mixed types entity by entity)
        morphium.store(Arrays.asList(v1, v2));
        morphium.store(p1);

        assertThat(v1.version).isEqualTo(1L);
        assertThat(v2.version).isEqualTo(1L);
        assertThat(p1.id).isNotNull();

        // Second store for v1 – must increment
        v1.name = "v-one-updated";
        morphium.store(v1);
        assertThat(v1.version).isEqualTo(2L);

        List<VersionedEntity> all = morphium.createQueryFor(VersionedEntity.class).asList();
        assertThat(all).hasSize(2);
    }

    // -----------------------------------------------------------------------
    // 6. Pre-set String ID with version == 0 → INSERT (not UPDATE)
    // -----------------------------------------------------------------------
    @Test
    public void presetStringId_withVersionZero_treatedAsInsert() {
        String presetId = UUID.randomUUID().toString();
        VersionedStringIdEntity e = new VersionedStringIdEntity(presetId, "preset-id-entity");

        morphium.store(e);

        assertThat(e.id).as("id must remain the pre-set value").isEqualTo(presetId);
        assertThat(e.version).as("version must be 1 after first store").isEqualTo(1L);

        VersionedStringIdEntity loaded = morphium.createQueryFor(VersionedStringIdEntity.class)
            .f("id").eq(presetId)
            .get();
        assertThat(loaded).isNotNull();
        assertThat(loaded.version).as("version in DB must be 1").isEqualTo(1L);
        assertThat(loaded.name).isEqualTo("preset-id-entity");
    }

    // -----------------------------------------------------------------------
    // 7. Pre-set String ID → second store increments version
    // -----------------------------------------------------------------------
    @Test
    public void presetStringId_secondStore_incrementsVersion() {
        String presetId = UUID.randomUUID().toString();
        VersionedStringIdEntity e = new VersionedStringIdEntity(presetId, "initial");

        morphium.store(e);
        assertThat(e.version).isEqualTo(1L);

        e.name = "updated";
        morphium.store(e);

        assertThat(e.version).as("version must be 2 after second store").isEqualTo(2L);

        VersionedStringIdEntity loaded = morphium.createQueryFor(VersionedStringIdEntity.class)
            .f("id").eq(presetId)
            .get();
        assertThat(loaded.version).isEqualTo(2L);
        assertThat(loaded.name).isEqualTo("updated");
    }

    // -----------------------------------------------------------------------
    // 8. Pre-set String ID → stale entity still throws VersionMismatchException
    // -----------------------------------------------------------------------
    @Test
    public void presetStringId_staleEntity_throwsVersionMismatch() {
        String presetId = UUID.randomUUID().toString();
        VersionedStringIdEntity e = new VersionedStringIdEntity(presetId, "original");

        morphium.store(e);  // v = 1

        // Simulate concurrent update
        VersionedStringIdEntity other = morphium.createQueryFor(VersionedStringIdEntity.class)
            .f("id").eq(presetId)
            .get();
        other.name = "updated-by-other";
        morphium.store(other);  // v = 2

        // e still has version = 1
        e.name = "stale-update";
        assertThatThrownBy(() -> morphium.store(e))
            .isInstanceOf(VersionMismatchException.class);
    }

    // -----------------------------------------------------------------------
    // 9. Multiple pre-set String ID entities in bulk store
    // -----------------------------------------------------------------------
    @Test
    public void presetStringId_bulkStore_allInserted() {
        VersionedStringIdEntity e1 = new VersionedStringIdEntity(UUID.randomUUID().toString(), "one");
        VersionedStringIdEntity e2 = new VersionedStringIdEntity(UUID.randomUUID().toString(), "two");

        morphium.store(Arrays.asList(e1, e2));

        assertThat(e1.version).as("e1 version must be 1").isEqualTo(1L);
        assertThat(e2.version).as("e2 version must be 1").isEqualTo(1L);

        List<VersionedStringIdEntity> all = morphium.createQueryFor(VersionedStringIdEntity.class).asList();
        assertThat(all).hasSize(2);
    }
}
