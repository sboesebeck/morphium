package de.caluga.test.morphium;

import de.caluga.morphium.FailedStore;
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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@code storeList(List, String, boolean continueOnError)}.
 * All tests run with InMemoryDriver — no MongoDB required.
 */
@Tag("inmemory")
public class StoreListContinueOnErrorTest {

    @Entity(collectionName = "versioned_item")
    public static class VersionedItem {
        @Id
        public MorphiumId id;

        @Version
        @Property(fieldName = "version")
        public long version;

        public String name;

        public VersionedItem() {}

        public VersionedItem(String name) {
            this.name = name;
        }
    }

    @Entity(collectionName = "plain_item")
    public static class PlainItem {
        @Id
        public MorphiumId id;

        public String value;

        public PlainItem() {}

        public PlainItem(String value) {
            this.value = value;
        }
    }

    private Morphium morphium;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig("storelist_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    // -----------------------------------------------------------------------
    // 1. Empty list returns empty failures
    // -----------------------------------------------------------------------
    @Test
    public void emptyList_returnsEmptyFailures() {
        List<VersionedItem> empty = new ArrayList<>();
        List<FailedStore<VersionedItem>> failures = morphium.storeList(empty, true);
        assertThat(failures).isEmpty();
    }

    // -----------------------------------------------------------------------
    // 2. All new entities — all succeed, no failures
    // -----------------------------------------------------------------------
    @Test
    public void allNewEntities_noFailures() {
        List<VersionedItem> items = List.of(
            new VersionedItem("a"),
            new VersionedItem("b"),
            new VersionedItem("c")
        );

        List<FailedStore<VersionedItem>> failures = morphium.storeList(items, true);

        assertThat(failures).isEmpty();
        for (VersionedItem item : items) {
            assertThat(item.id).isNotNull();
            assertThat(item.version).isEqualTo(1L);
            // Verify round-trip: version in DB matches in-memory
            VersionedItem fromDb = morphium.createQueryFor(VersionedItem.class)
                .f("id").eq(item.id).get();
            assertThat(fromDb).isNotNull();
            assertThat(fromDb.version).isEqualTo(1L);
        }
        // Verify persisted
        long count = morphium.createQueryFor(VersionedItem.class).countAll();
        assertThat(count).isEqualTo(3);
    }

    // -----------------------------------------------------------------------
    // 3. continueOnError=true: one stale entity, rest succeed
    // -----------------------------------------------------------------------
    @Test
    public void continueOnError_collectsVersionConflict() {
        // Store 3 entities
        VersionedItem a = new VersionedItem("a");
        VersionedItem b = new VersionedItem("b");
        VersionedItem c = new VersionedItem("c");
        morphium.store(a);
        morphium.store(b);
        morphium.store(c);
        assertThat(a.version).isEqualTo(1L);
        assertThat(b.version).isEqualTo(1L);
        assertThat(c.version).isEqualTo(1L);

        // Simulate concurrent modification: update b behind our back
        VersionedItem bConcurrent = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(b.id).get();
        bConcurrent.name = "b-concurrent";
        morphium.store(bConcurrent); // b is now version 2 in DB

        // Our 'b' still has version 1 — stale
        a.name = "a-updated";
        b.name = "b-stale-update";
        c.name = "c-updated";

        List<VersionedItem> batch = List.of(a, b, c);
        List<FailedStore<VersionedItem>> failures = morphium.storeList(batch, true);

        // b should have failed
        assertThat(failures).hasSize(1);
        FailedStore<VersionedItem> failed = failures.get(0);
        assertThat(failed.getEntity()).isSameAs(b);
        assertThat(failed.getIndex()).isEqualTo(1); // index in original list
        assertThat(failed.getCause()).isInstanceOf(VersionMismatchException.class);

        // a and c should have been updated
        assertThat(a.version).isEqualTo(2L);
        assertThat(c.version).isEqualTo(2L);
        // b's version should NOT have been incremented
        assertThat(b.version).isEqualTo(1L);

        // Verify in DB
        VersionedItem aDb = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(a.id).get();
        assertThat(aDb.name).isEqualTo("a-updated");
        assertThat(aDb.version).isEqualTo(2L);

        VersionedItem bDb = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(b.id).get();
        assertThat(bDb.name).isEqualTo("b-concurrent"); // not our stale update
        assertThat(bDb.version).isEqualTo(2L);
    }

    // -----------------------------------------------------------------------
    // 4. continueOnError=false: throws VersionMismatchException with listIndex
    // -----------------------------------------------------------------------
    @Test
    public void failFast_throwsVersionMismatchWithIndex() {
        VersionedItem a = new VersionedItem("a");
        VersionedItem b = new VersionedItem("b");
        morphium.store(a);
        morphium.store(b);

        // Make 'a' stale
        VersionedItem aConcurrent = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(a.id).get();
        aConcurrent.name = "a-concurrent";
        morphium.store(aConcurrent);

        a.name = "a-stale";
        b.name = "b-ok";
        List<VersionedItem> batch = List.of(a, b);

        assertThatThrownBy(() -> morphium.storeList(batch, false))
            .isInstanceOf(VersionMismatchException.class)
            .satisfies(ex -> {
                VersionMismatchException vme = (VersionMismatchException) ex;
                assertThat(vme.getListIndex()).isEqualTo(0);
                assertThat(vme.getExpectedVersion()).isEqualTo(1L);
            });
    }

    // -----------------------------------------------------------------------
    // 5. All entities fail — all collected in failures list
    // -----------------------------------------------------------------------
    @Test
    public void allEntitiesFail_allInFailuresList() {
        VersionedItem a = new VersionedItem("a");
        VersionedItem b = new VersionedItem("b");
        morphium.store(a);
        morphium.store(b);

        // Make both stale
        VersionedItem aCon = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(a.id).get();
        aCon.name = "a-con";
        morphium.store(aCon);

        VersionedItem bCon = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(b.id).get();
        bCon.name = "b-con";
        morphium.store(bCon);

        a.name = "a-stale";
        b.name = "b-stale";

        List<FailedStore<VersionedItem>> failures = morphium.storeList(List.of(a, b), true);

        assertThat(failures).hasSize(2);
        assertThat(failures.get(0).getIndex()).isEqualTo(0);
        assertThat(failures.get(0).getEntity()).isSameAs(a);
        assertThat(failures.get(1).getIndex()).isEqualTo(1);
        assertThat(failures.get(1).getEntity()).isSameAs(b);
    }

    // -----------------------------------------------------------------------
    // 6. Mixed new + versioned-update entities in one batch
    // -----------------------------------------------------------------------
    @Test
    public void mixedNewAndUpdate_handledCorrectly() {
        // Store one existing entity
        VersionedItem existing = new VersionedItem("existing");
        morphium.store(existing);
        assertThat(existing.version).isEqualTo(1L);

        // Create a new entity
        VersionedItem brandNew = new VersionedItem("brand-new");

        // Update existing
        existing.name = "existing-updated";

        List<VersionedItem> batch = List.of(brandNew, existing);
        List<FailedStore<VersionedItem>> failures = morphium.storeList(batch, true);

        assertThat(failures).isEmpty();
        assertThat(brandNew.id).isNotNull();
        assertThat(brandNew.version).isEqualTo(1L);
        assertThat(existing.version).isEqualTo(2L);

        long count = morphium.createQueryFor(VersionedItem.class).countAll();
        assertThat(count).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // 7. Non-versioned entities with storeList — should work like normal store
    // -----------------------------------------------------------------------
    @Test
    public void nonVersionedEntities_storeNormally() {
        PlainItem p1 = new PlainItem("one");
        PlainItem p2 = new PlainItem("two");

        List<FailedStore<PlainItem>> failures = morphium.storeList(List.of(p1, p2), true);

        assertThat(failures).isEmpty();
        assertThat(p1.id).isNotNull();
        assertThat(p2.id).isNotNull();

        long count = morphium.createQueryFor(PlainItem.class).countAll();
        assertThat(count).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // 8. Convenience method storeList(List, boolean) delegates correctly
    // -----------------------------------------------------------------------
    @Test
    public void convenienceMethod_worksWithoutCollectionName() {
        VersionedItem item = new VersionedItem("test");
        List<FailedStore<VersionedItem>> failures = morphium.storeList(List.of(item), true);

        assertThat(failures).isEmpty();
        assertThat(item.id).isNotNull();
        assertThat(item.version).isEqualTo(1L);
        // Round-trip verification
        VersionedItem fromDb = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(item.id).get();
        assertThat(fromDb).isNotNull();
        assertThat(fromDb.version).isEqualTo(1L);
    }

    // -----------------------------------------------------------------------
    // 9. Version conflict with null collection name produces correct FailedStore
    // -----------------------------------------------------------------------
    @Test
    public void versionConflict_withNullCollection_producesCorrectFailure() {
        VersionedItem item = new VersionedItem("original");
        morphium.store(item);

        // Make stale
        VersionedItem concurrent = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(item.id).get();
        concurrent.name = "concurrent";
        morphium.store(concurrent);

        item.name = "stale";
        List<FailedStore<VersionedItem>> failures = morphium.storeList(List.of(item), null, true);

        assertThat(failures).hasSize(1);
        assertThat(failures.get(0).getIndex()).isEqualTo(0);
        assertThat(failures.get(0).getEntity()).isSameAs(item);
        assertThat(failures.get(0).getCause()).isInstanceOf(VersionMismatchException.class);
    }

    // -----------------------------------------------------------------------
    // 10. Fail-fast with stale entity at list index > 0
    // -----------------------------------------------------------------------
    @Test
    public void failFast_staleAtIndexOne_throwsWithCorrectIndex() {
        VersionedItem a = new VersionedItem("a");
        VersionedItem b = new VersionedItem("b");
        morphium.store(a);
        morphium.store(b);

        // Make 'b' (index 1) stale
        VersionedItem bConcurrent = morphium.createQueryFor(VersionedItem.class)
            .f("id").eq(b.id).get();
        bConcurrent.name = "b-concurrent";
        morphium.store(bConcurrent);

        a.name = "a-ok";
        b.name = "b-stale";
        List<VersionedItem> batch = List.of(a, b);

        assertThatThrownBy(() -> morphium.storeList(batch, false))
            .isInstanceOf(VersionMismatchException.class)
            .satisfies(ex -> {
                VersionMismatchException vme = (VersionMismatchException) ex;
                assertThat(vme.getListIndex()).isEqualTo(1);
                assertThat(vme.getExpectedVersion()).isEqualTo(1L);
            });
    }
}
