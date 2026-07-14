package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test: $in / $nin operands supplied as a Java array (e.g. a String[]
 * passed into a rawQuery) previously caused a ClassCastException ("[Ljava.lang.String;
 * cannot be cast to java.util.List") in the InMemoryDriver. They must be accepted
 * just like a List.
 */
@Tag("inmemory")
public class InMemInArrayOperandTest {

    private InMemoryDriver newDriver() {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    @Test
    public void testInWithStringArrayOperand() throws Exception {
        var drv = newDriver();
        String db = "in_array_db";
        String coll = "items";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", "a", "v", 1),
            Doc.of("_id", "b", "v", 2),
            Doc.of("_id", "c", "v", 3))).execute();

        // $in operand is a Java String[] (not a List) - as produced by rawQuery callers
        String[] ids = new String[]{"a", "c"};
        List<Map<String, Object>> found = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", Doc.of("$in", ids))).execute();

        assertEquals(2, found.size(), "$in with String[] must match like a List");
        assertThat(found).extracting(d -> d.get("_id")).containsExactlyInAnyOrder("a", "c");
    }

    @Test
    public void testNinWithStringArrayOperand() throws Exception {
        var drv = newDriver();
        String db = "in_array_db";
        String coll = "items2";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", "a", "v", 1),
            Doc.of("_id", "b", "v", 2),
            Doc.of("_id", "c", "v", 3))).execute();

        String[] ids = new String[]{"a", "c"};
        List<Map<String, Object>> found = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", Doc.of("$nin", ids))).execute();

        assertEquals(1, found.size());
        assertThat(found.get(0).get("_id")).isEqualTo("b");
    }

    @Test
    public void testUpdateWithInStringArrayOperand() throws Exception {
        var drv = newDriver();
        String db = "in_array_db";
        String coll = "items3";
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", "a", "flag", false),
            Doc.of("_id", "b", "flag", false),
            Doc.of("_id", "c", "flag", false))).execute();

        // Mirrors UpdateRulesRepository: rawQuery {_id: {$in: String[]}} + $set, multi
        String[] ids = new String[]{"a", "b"};
        Map<String, Object> res = new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of(
                "q", Doc.of("_id", Doc.of("$in", ids)),
                "u", Doc.of("$set", Doc.of("flag", true)),
                "multi", true))
            .execute();

        assertEquals(2, ((Number) res.get("n")).intValue());
        assertEquals(2, ((Number) res.get("nModified")).intValue());
    }
}
