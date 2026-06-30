package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@code $setOnInsert} support in the {@link InMemoryDriver}.
 *
 * <p>{@code $setOnInsert} must apply its fields only when an upsert results in an INSERT and be a
 * no-op on a pure update. This lets callers seed insert-only fields — a String {@code _id}, an
 * initial {@code @Version} value, an immutable {@code creationSource} — without overwriting them on
 * subsequent updates. It is the building block for an atomic upsert-on-natural-key that replaces a
 * non-atomic find-then-save (check-then-act) pattern.
 */
@Tag("inmemory")
public class InMemSetOnInsertTest {

    private static final String DB = "set_on_insert_db";
    private static final String COLL = "campaigns";

    private InMemoryDriver driver;

    @BeforeEach
    public void setUp() throws Exception {
        driver = new InMemoryDriver();
        driver.connect();
        new ClearCollectionCommand(driver).setDb(DB).setColl(COLL).doClear();
    }

    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void setOnInsertAppliesFieldsOnInsert() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "A0001");
        Map<String, Object> update = Doc.of(
                "$set", Doc.of("publicationDate", "2026-06-01"),
                "$setOnInsert", Doc.of("_id", "fixed-id-1", "version", 1L, "creationSource", "AUTOMATIC"));

        Map<String, Object> result = upsert(filter, update);

        assertThat(result.get("upserted")).as("first upsert must insert").isNotNull();

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).as("_id must be seeded by $setOnInsert, not generated").isEqualTo("fixed-id-1");
        assertThat(stored.get("version")).isEqualTo(1L);
        assertThat(stored.get("creationSource")).isEqualTo("AUTOMATIC");
        assertThat(stored.get("campaignNumber")).as("query equality predicate is still seeded").isEqualTo("A0001");
        assertThat(stored.get("publicationDate")).as("$set is also applied on insert").isEqualTo("2026-06-01");
    }

    @Test
    public void setOnInsertIsIgnoredOnUpdate() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "A0002");

        // 1) insert with creationSource=MANUAL
        upsert(filter, Doc.of(
                "$set", Doc.of("publicationDate", "2026-06-01"),
                "$setOnInsert", Doc.of("_id", "fixed-id-2", "version", 1L, "creationSource", "MANUAL")));

        // 2) second upsert hits the existing document — $setOnInsert MUST NOT overwrite creationSource,
        //    while $set MUST update publicationDate.
        upsert(filter, Doc.of(
                "$set", Doc.of("publicationDate", "2026-07-15"),
                "$setOnInsert", Doc.of("_id", "should-be-ignored", "version", 99L, "creationSource", "AUTOMATIC")));

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).as("_id must not change on update").isEqualTo("fixed-id-2");
        assertThat(stored.get("version")).as("version must not be touched by $setOnInsert on update").isEqualTo(1L);
        assertThat(stored.get("creationSource"))
                .as("creationSource is insert-only — must survive a later update")
                .isEqualTo("MANUAL");
        assertThat(stored.get("publicationDate")).as("$set must still apply on update").isEqualTo("2026-07-15");
    }

    @Test
    public void setOnInsertWithoutSetStillUpdatesNothingExtraOnUpdate() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "A0003");

        upsert(filter, Doc.of("$setOnInsert", Doc.of("_id", "fixed-id-3", "creationSource", "AUTOMATIC")));
        upsert(filter, Doc.of("$setOnInsert", Doc.of("_id", "x", "creationSource", "MANUAL")));

        assertThat(find(Doc.of())).as("must remain a single document").hasSize(1);
        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).isEqualTo("fixed-id-3");
        assertThat(stored.get("creationSource")).isEqualTo("AUTOMATIC");
    }

    @Test
    public void findAndModifyUpsertInsertsAndSeedsSetOnInsert() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "F0001");
        // version is incremented via $inc only (1 on insert) — $setOnInsert must NOT also target it,
        // mirroring the application upsert; real mongod rejects a field appearing in both operators.
        Map<String, Object> update = Doc.of(
                "$set", Doc.of("publicationDate", "2026-06-01"),
                "$setOnInsert", Doc.of("_id", "famid-1", "creationSource", "AUTOMATIC"),
                "$inc", Doc.of("version", 1L));

        // newFlag=true must return the inserted document
        Map<String, Object> returned = findAndModify(filter, update, true, true);

        assertThat(returned).as("findAndModify upsert must return the inserted document").isNotNull();
        assertThat(returned.get("_id")).isEqualTo("famid-1");
        assertThat(returned.get("creationSource")).isEqualTo("AUTOMATIC");
        assertThat(returned.get("publicationDate")).isEqualTo("2026-06-01");
        assertThat(returned.get("version")).as("$inc seeds version=1 on insert").isEqualTo(1L);

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).isEqualTo("famid-1");
    }

    @Test
    public void findAndModifyUpsertInsertWithNewFalseReturnsNull() throws Exception {
        // An insert has no pre-image: with new:false (the default) MongoDB returns null.
        Map<String, Object> filter = Doc.of("campaignNumber", "F0004");
        Map<String, Object> returned = findAndModify(filter, Doc.of(
                "$setOnInsert", Doc.of("_id", "famid-4", "creationSource", "AUTOMATIC")), true, false);

        assertThat(returned).as("upsert-insert with new:false must return null").isNull();
        // but the document was still inserted
        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).isEqualTo("famid-4");
        assertThat(stored.get("creationSource")).isEqualTo("AUTOMATIC");
    }

    @Test
    public void findAndModifyUpdateWithNewFalseReturnsPreImage() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "F0005");
        findAndModify(filter, Doc.of("$set", Doc.of("status", "before")), true, true);

        // new:false must return the document as it was BEFORE this update
        Map<String, Object> returned = findAndModify(filter,
                Doc.of("$set", Doc.of("status", "after")), false, false);

        assertThat(returned.get("status")).as("new:false returns the pre-image").isEqualTo("before");
        assertThat(onlyDocument().get("status")).as("store holds the post-update value").isEqualTo("after");
    }

    @Test
    public void findAndModifyReturnedDocumentIsNotLiveReference() throws Exception {
        // Mutating the returned document must not corrupt the stored map (all branches deepClone).
        Map<String, Object> filter = Doc.of("campaignNumber", "F0006");
        Map<String, Object> returned = findAndModify(filter, Doc.of(
                "$setOnInsert", Doc.of("_id", "famid-6", "creationSource", "AUTOMATIC")), true, true);

        returned.put("creationSource", "TAMPERED");
        assertThat(onlyDocument().get("creationSource"))
                .as("mutating the returned doc must not leak into the store")
                .isEqualTo("AUTOMATIC");
    }

    @Test
    public void findAndModifyUpsertMergesIntoExistingAndReturnsNew() throws Exception {
        Map<String, Object> filter = Doc.of("campaignNumber", "F0002");

        findAndModify(filter, Doc.of(
                "$set", Doc.of("publicationDate", "2026-06-01"),
                "$setOnInsert", Doc.of("_id", "famid-2", "creationSource", "AUTOMATIC")), true, true);

        // second call merges (description) — creationSource must survive, newFlag returns post-state
        Map<String, Object> returned = findAndModify(filter, Doc.of(
                "$set", Doc.of("campaignDescription", "merged"),
                "$setOnInsert", Doc.of("_id", "ignored", "creationSource", "MANUAL")), true, true);

        assertThat(returned.get("_id")).isEqualTo("famid-2");
        assertThat(returned.get("creationSource")).isEqualTo("AUTOMATIC");
        assertThat(returned.get("campaignDescription")).isEqualTo("merged");
        assertThat(returned.get("publicationDate")).isEqualTo("2026-06-01");
        assertThat(find(Doc.of())).hasSize(1);
    }

    @Test
    public void findAndModifyWithoutUpsertReturnsNullWhenAbsent() throws Exception {
        Map<String, Object> returned = findAndModify(Doc.of("campaignNumber", "missing"),
                Doc.of("$set", Doc.of("x", 1)), false, true);
        assertThat(returned).isNull();
        assertThat(find(Doc.of())).isEmpty();
    }

    /**
     * Regression test for #202: {@code runCommand(FindAndModifyMongoCommand)} must honour the
     * {@code upsert} flag so that an equality predicate nested inside {@code $and} seeds the
     * upserted {@code _id} (mirroring MongoDB), and a later delete by that fixed {@code _id} matches.
     *
     * <p>Reproduces the quarkus-morphium migration-lock scenario exactly via the findAndModify path
     * (the update-command path is covered by {@code InMemUpsertAndExtractionTest}). Before the fix in
     * #203, findAndModify ignored {@code upsert}, so nothing was inserted at all.
     */
    @Test
    public void findAndModifyUpsertSeedsIdFromNestedAndFilterAndIsDeletable() throws Exception {
        Date now = new Date();
        // {$and:[{_id:"migration_lock"},{expires_at:{$lte:now}}]} — exactly what the lock acquire builds
        Map<String, Object> filter = Doc.of("$and", List.of(
                Doc.of("_id", "migration_lock"),
                Doc.of("expires_at", Doc.of("$lte", now))));

        // upsert with new:false: an insert has no pre-image, so MongoDB (and now InMem) returns null
        Map<String, Object> returned = findAndModify(filter, Doc.of("$set", Doc.of("owner", "owner-1")),
                true, false);
        assertThat(returned).as("upsert-insert with new:false returns null").isNull();

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id"))
                .as("_id must be seeded from the $and-nested equality, not a generated ObjectId")
                .isEqualTo("migration_lock");
        assertThat(stored.get("owner")).isEqualTo("owner-1");

        // the fixed _id must be deletable — before the fix the upsert never ran, so this is the
        // load-bearing assertion for the migration-lock release path
        deleteById("migration_lock");
        assertThat(find(Doc.of())).as("delete by the fixed _id must remove the upserted lock").isEmpty();
    }

    // --- helpers -------------------------------------------------------------

    private Map<String, Object> findAndModify(Map<String, Object> filter, Map<String, Object> update,
                                              boolean upsert, boolean newFlag) throws Exception {
        return new FindAndModifyMongoCommand(driver)
                .setDb(DB).setColl(COLL)
                .setQuery(filter).setUpdate(update)
                .setUpsert(upsert).setNewFlag(newFlag)
                .execute();
    }

    private Map<String, Object> upsert(Map<String, Object> filter, Map<String, Object> update) throws Exception {
        return new UpdateMongoCommand(driver)
                .setDb(DB).setColl(COLL)
                .addUpdate(Doc.of("q", filter, "u", update, "upsert", true))
                .execute();
    }

    private List<Map<String, Object>> find(Map<String, Object> filter) throws Exception {
        return new FindCommand(driver).setDb(DB).setColl(COLL).setFilter(filter).execute();
    }

    private void deleteById(Object id) throws Exception {
        // limit 1: deleting by a unique _id matches at most one document
        new DeleteMongoCommand(driver).setDb(DB).setColl(COLL)
                .addDelete(Doc.of("_id", id), 1, null, null)
                .execute();
    }

    private Map<String, Object> onlyDocument() throws Exception {
        List<Map<String, Object>> all = find(Doc.of());
        assertThat(all).hasSize(1);
        return all.get(0);
    }
}
