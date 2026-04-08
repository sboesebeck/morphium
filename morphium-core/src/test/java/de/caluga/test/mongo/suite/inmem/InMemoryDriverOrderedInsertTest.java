package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ordered vs. unordered insert semantics in InMemoryDriver.
 *
 * <p>With {@code ordered=true} (the default), the first duplicate _id or unique
 * index violation causes an immediate {@link MorphiumDriverException}.
 *
 * <p>With {@code ordered=false}, violations are collected as structured
 * {@code writeErrors} ({@code index}, {@code code}, {@code errmsg}) and
 * processing continues with the remaining documents.
 */
@Tag("inmemory")
class InMemoryDriverOrderedInsertTest extends MorphiumInMemTestBase {

    private static final String DB = "test";
    private static final String COLL = "ordered_insert_test";

    @Test
    void orderedInsert_throwsOnDuplicateId() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();
        var id = new ObjectId();

        driver.insert(DB, COLL, List.of(Doc.of("_id", id, "value", 1)), null, true);

        assertThatThrownBy(() ->
            driver.insert(DB, COLL, List.of(Doc.of("_id", id, "value", 2)), null, true)
        ).isInstanceOf(MorphiumDriverException.class)
         .hasMessageContaining("Duplicate");
    }

    @Test
    void unorderedInsert_collectsDuplicateIdAsWriteError() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();
        var existingId = new ObjectId();
        var newId = new ObjectId();

        driver.insert(DB, COLL, List.of(Doc.of("_id", existingId, "value", 1)), null, true);

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", existingId, "value", "duplicate"));
        docs.add(Doc.of("_id", newId, "value", "valid"));

        List<Map<String, Object>> writeErrors = driver.insert(DB, COLL, docs, null, false);

        assertThat(writeErrors).hasSize(1);
        assertThat(writeErrors.get(0).get("code")).isEqualTo(11000);
        assertThat(writeErrors.get(0).get("errmsg").toString()).contains("dup key");

        // The valid document should have been inserted
        var results = driver.find(DB, COLL, Doc.of("_id", newId), null, null, 0, 1);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).get("value")).isEqualTo("valid");
    }

    @Test
    void unorderedInsert_continuesAfterMultipleDuplicates() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();
        var id1 = new ObjectId();
        var id2 = new ObjectId();

        driver.insert(DB, COLL, List.of(
            Doc.of("_id", id1, "value", "existing1"),
            Doc.of("_id", id2, "value", "existing2")
        ), null, true);

        var newId = new ObjectId();
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", id1, "value", "dup1"));
        docs.add(Doc.of("_id", newId, "value", "new"));
        docs.add(Doc.of("_id", id2, "value", "dup2"));

        List<Map<String, Object>> writeErrors = driver.insert(DB, COLL, docs, null, false);

        assertThat(writeErrors).hasSize(2);
        assertThat(writeErrors).allSatisfy(err ->
            assertThat((int) err.get("code")).isEqualTo(11000)
        );

        // Only the non-duplicate should be in the collection
        var results = driver.find(DB, COLL, Doc.of("_id", newId), null, null, 0, 1);
        assertThat(results).hasSize(1);
    }

    @Test
    void unorderedInsert_noDuplicates_insertsAll() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();

        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("value", "a"));
        docs.add(Doc.of("value", "b"));
        docs.add(Doc.of("value", "c"));

        List<Map<String, Object>> writeErrors = driver.insert(DB, COLL, docs, null, false);

        assertThat(writeErrors).isEmpty();
        var all = driver.find(DB, COLL, Doc.of(), null, null, 0, 100);
        assertThat(all).hasSize(3);
    }

    @Test
    void orderedInsert_viaCommand_stopsAtFirstDuplicate() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();
        var existingId = new ObjectId();

        driver.insert(DB, COLL, List.of(Doc.of("_id", existingId, "value", 1)), null, true);

        var cmd = new InsertMongoCommand(driver.getPrimaryConnection(null));
        cmd.setDb(DB);
        cmd.setColl(COLL);
        cmd.setDocuments(List.of(
            Doc.of("_id", existingId, "value", "dup"),
            Doc.of("value", "should-not-be-inserted")
        ));
        cmd.setOrdered(true);

        assertThatThrownBy(cmd::execute)
            .isInstanceOf(MorphiumDriverException.class);
    }

    @Test
    void unorderedInsert_viaCommand_collectsErrorsAndInserts() throws Exception {
        var driver = (InMemoryDriver) morphium.getDriver();
        var existingId = new ObjectId();
        var newId = new ObjectId();

        driver.insert(DB, COLL, List.of(Doc.of("_id", existingId, "value", 1)), null, true);

        var con = driver.getPrimaryConnection(null);
        var cmd = new InsertMongoCommand(con);
        cmd.setDb(DB);
        cmd.setColl(COLL);
        cmd.setDocuments(new ArrayList<>(List.of(
            Doc.of("_id", existingId, "value", "dup"),
            Doc.of("_id", newId, "value", "valid")
        )));
        cmd.setOrdered(false);

        // InsertMongoCommand.execute() throws on writeErrors but the valid doc is still inserted
        assertThatThrownBy(cmd::execute)
            .isInstanceOf(MorphiumDriverException.class)
            .satisfies(ex -> {
                var driverEx = (MorphiumDriverException) ex;
                assertThat(driverEx.getWriteErrors()).hasSize(1);
                assertThat((int) driverEx.getWriteErrors().get(0).get("code")).isEqualTo(11000);
            });

        // The valid document should have been inserted despite the error
        var results = driver.find(DB, COLL, Doc.of("_id", newId), null, null, 0, 1);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).get("value")).isEqualTo("valid");
    }
}
