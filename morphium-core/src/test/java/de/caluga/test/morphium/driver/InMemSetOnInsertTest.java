package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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

    // --- helpers -------------------------------------------------------------

    private Map<String, Object> upsert(Map<String, Object> filter, Map<String, Object> update) throws Exception {
        return new UpdateMongoCommand(driver)
                .setDb(DB).setColl(COLL)
                .addUpdate(Doc.of("q", filter, "u", update, "upsert", true))
                .execute();
    }

    private List<Map<String, Object>> find(Map<String, Object> filter) throws Exception {
        return new FindCommand(driver).setDb(DB).setColl(COLL).setFilter(filter).execute();
    }

    private Map<String, Object> onlyDocument() throws Exception {
        List<Map<String, Object>> all = find(Doc.of());
        assertThat(all).hasSize(1);
        return all.get(0);
    }
}
