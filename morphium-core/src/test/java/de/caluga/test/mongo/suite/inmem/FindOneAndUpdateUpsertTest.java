package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for Query#findOneAndUpdate(Map, boolean, boolean) — atomic upsert support,
 * running against the InMemoryDriver (supports $setOnInsert + upsert/newFlag as of PR #203).
 */
public class FindOneAndUpdateUpsertTest extends MorphiumInMemTestBase {

    @Test
    public void upsertInsertsNewDocumentWithGeneratedStringId() {
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("X");

        Map<String, Object> update = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "X", "payload", "p1"),
                "$setOnInsert", UtilsMap.of("createdBy", "importer"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity ret = q.findOneAndUpdate(update, true, true);

        assertNotNull(ret, "Upsert-insert must return the newly created document (returnNew=true)");
        assertNotNull(ret.id, "id must be generated");
        assertEquals("X", ret.naturalKey);
        assertEquals("p1", ret.payload);
        assertEquals("importer", ret.createdBy);
        assertEquals(1L, ret.version);

        // id must be a String-UUID (MorphiumId.toString()), NOT null, NOT an ObjectId-hex-only surprise type
        assertEquals(24, ret.id.length(), "MorphiumId hex string is expected to be 24 chars");

        assertEquals(1, morphium.createQueryFor(UpsertTestEntity.class).countAll());
    }

    @Test
    public void upsertMergesExistingDocumentAndKeepsSetOnInsertFieldsUnchanged() {
        Query<UpsertTestEntity> q1 = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("Y");

        Map<String, Object> firstUpdate = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "Y", "payload", "initial"),
                "$setOnInsert", UtilsMap.of("createdBy", "importer"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity created = q1.findOneAndUpdate(firstUpdate, true, true);
        assertNotNull(created);
        String originalId = created.id;

        Query<UpsertTestEntity> q2 = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("Y");

        Map<String, Object> secondUpdate = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "Y", "payload", "merged"),
                "$setOnInsert", UtilsMap.of("createdBy", "shouldNotBeApplied"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity merged = q2.findOneAndUpdate(secondUpdate, true, true);

        assertNotNull(merged);
        assertEquals(originalId, merged.id, "_id must not change on merge");
        assertEquals("merged", merged.payload, "$set fields must be overwritten");
        assertEquals("importer", merged.createdBy, "$setOnInsert fields must stay untouched on existing doc");
        assertEquals(2L, merged.version, "version must be incremented");

        assertEquals(1, morphium.createQueryFor(UpsertTestEntity.class).countAll());
    }

    @Test
    public void callerProvidedIdInSetOnInsertIsNotOverwritten() {
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("Z");

        Map<String, Object> update = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "Z", "payload", "p1"),
                "$setOnInsert", UtilsMap.of("_id", "caller-defined-id"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity ret = q.findOneAndUpdate(update, true, true);

        assertNotNull(ret);
        assertEquals("caller-defined-id", ret.id, "caller-provided _id in $setOnInsert must be respected");
    }

    @Test
    public void callerProvidedIdInQueryFilterIsNotOverwritten() {
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("_id").eq("filter-defined-id");

        Map<String, Object> update = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "W", "payload", "p1"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity ret = q.findOneAndUpdate(update, true, true);

        assertNotNull(ret);
        assertEquals("filter-defined-id", ret.id, "caller-provided _id in query filter must be respected");
    }

    @Test
    public void plainFindOneAndUpdateWithoutUpsertBehavesAsBefore() {
        // no document exists -> must return null, must NOT create a document (no upsert)
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("does-not-exist");

        Map<String, Object> update = UtilsMap.of("$set", UtilsMap.of("payload", "p1"));

        UpsertTestEntity ret = q.findOneAndUpdate(update);
        assertNull(ret, "Without upsert, no match must return null");
        assertEquals(0, morphium.createQueryFor(UpsertTestEntity.class).countAll());

        // now with an existing doc: returned document must be the OLD state (returnNew=false, matches previous default)
        UpsertTestEntity existing = new UpsertTestEntity();
        existing.id = "existing-1";
        existing.naturalKey = "known";
        existing.payload = "before";
        morphium.store(existing);
        waitForWrites();

        Query<UpsertTestEntity> q2 = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("known");
        Map<String, Object> update2 = UtilsMap.of("$set", UtilsMap.of("payload", "after"));
        UpsertTestEntity retOld = q2.findOneAndUpdate(update2);

        assertNotNull(retOld);
        assertEquals("before", retOld.payload, "findOneAndUpdate(Map) must keep returning the document as it was BEFORE the update");

        UpsertTestEntity reread = morphium.createQueryFor(UpsertTestEntity.class).f("naturalKey").eq("known").get();
        assertEquals("after", reread.payload, "the update itself must still have been applied in the DB");
    }

    @Test
    public void callerProvidedIdInSetIsNotOverwritten() {
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("V");

        Map<String, Object> update = UtilsMap.of(
                "$set", UtilsMap.of("_id", "set-defined-id", "naturalKey", "V", "payload", "p1"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity ret = q.findOneAndUpdate(update, true, true);

        assertNotNull(ret);
        assertEquals("set-defined-id", ret.id, "caller-provided _id in $set must be respected, not overwritten by a generated id");
    }

    @Test
    public void upsertInsertWithReturnNewFalseReturnsNullButStillCreatesDocument() {
        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("U");

        Map<String, Object> update = UtilsMap.of(
                "$set", UtilsMap.of("naturalKey", "U", "payload", "p1"),
                "$inc", UtilsMap.of("version", 1L));

        UpsertTestEntity ret = q.findOneAndUpdate(update, true, false);

        assertNull(ret, "returnNew=false on an upsert-insert must return null (no document existed BEFORE the update)");

        UpsertTestEntity stored = morphium.createQueryFor(UpsertTestEntity.class).f("naturalKey").eq("U").get();
        assertNotNull(stored, "the document must have been created despite the null return value");
        assertNotNull(stored.id, "id must have been generated");
        assertEquals(24, stored.id.length(), "MorphiumId hex string is expected to be 24 chars");
    }

    @Test
    public void sortDeterminesWhichMatchingDocumentIsUpdated() {
        // Three documents share the same group -> without a sort, MongoDB would update an
        // arbitrary one. With sort("-priority") the highest-priority document must be updated.
        for (int i = 1; i <= 3; i++) {
            UpsertTestEntity e = new UpsertTestEntity();
            e.id = "doc-" + i;
            e.naturalKey = "group";
            e.priority = i;
            e.payload = "untouched";
            morphium.store(e);
        }
        waitForWrites();

        Query<UpsertTestEntity> q = morphium.createQueryFor(UpsertTestEntity.class)
                .f("naturalKey").eq("group")
                .sort("-priority");

        Map<String, Object> update = UtilsMap.of("$set", UtilsMap.of("payload", "updated"));
        UpsertTestEntity updated = q.findOneAndUpdate(update, false, true);

        assertNotNull(updated);
        assertEquals("doc-3", updated.id, "sort(\"-priority\") must select the highest-priority document");
        assertEquals("updated", updated.payload);

        // the other two must be untouched
        assertEquals("untouched", morphium.createQueryFor(UpsertTestEntity.class).f("_id").eq("doc-1").get().payload);
        assertEquals("untouched", morphium.createQueryFor(UpsertTestEntity.class).f("_id").eq("doc-2").get().payload);
    }

    @Test
    public void upsertInvalidatesReadCacheForCachedEntity() {
        CachedUpsertTestEntity existing = new CachedUpsertTestEntity();
        existing.id = "cache-1";
        existing.naturalKey = "cachedGroup";
        existing.payload = "before";
        morphium.store(existing);
        waitForWrites();

        // populate the read cache with the pre-upsert state
        CachedUpsertTestEntity cachedRead = morphium.createQueryFor(CachedUpsertTestEntity.class)
                .f("naturalKey").eq("cachedGroup").get();
        assertNotNull(cachedRead);
        assertEquals("before", cachedRead.payload);

        Query<CachedUpsertTestEntity> q = morphium.createQueryFor(CachedUpsertTestEntity.class)
                .f("naturalKey").eq("cachedGroup");
        Map<String, Object> update = UtilsMap.of("$set", UtilsMap.of("payload", "after"));
        CachedUpsertTestEntity updated = q.findOneAndUpdate(update, true, true);
        assertNotNull(updated);
        assertEquals("after", updated.payload);

        // a fresh read for the same query must observe the update, not a stale cache entry
        CachedUpsertTestEntity reread = morphium.createQueryFor(CachedUpsertTestEntity.class)
                .f("naturalKey").eq("cachedGroup").get();
        assertNotNull(reread);
        assertEquals("after", reread.payload, "findOneAndUpdate must invalidate the read cache for @Cache entities");
    }

    @Entity(translateCamelCase = false)
    public static class UpsertTestEntity {
        @Id
        public String id;
        public String naturalKey;
        public String payload;
        public String createdBy;
        public int priority;
        @Version
        public Long version;
    }

    @Entity(translateCamelCase = false)
    @Cache(readCache = true, clearOnWrite = true)
    public static class CachedUpsertTestEntity {
        @Id
        public String id;
        public String naturalKey;
        public String payload;
    }
}
