package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for $pop array update operator
 */
@SuppressWarnings("unchecked")
@Tag("core")
public class ArrayPopTest {
    private static final String db = "test_db";

    @Test
    public void testPopLast() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        String collection = "test_pop_last";

        // Insert test document with array
        InsertMongoCommand insert = new InsertMongoCommand(drv).setDb(db).setColl(collection);
        insert.setDocuments(Arrays.asList(Doc.of("_id", 1, "scores", Arrays.asList(10, 20, 30, 40, 50))));
        insert.execute();

        // Pop last element (1 means remove last)
        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(collection);
        update.addUpdate(Doc.of("_id", 1), Doc.of("$pop", Doc.of("scores", 1)),
                null, false, false, null, null, null);
        update.execute();

        // Verify
        List<Map<String, Object>> results = drv.find(db, collection, Doc.of("_id", 1), null, null, 0, 0);
        assertEquals(1, results.size());
        List<Integer> scores = (List<Integer>) results.get(0).get("scores");
        assertNotNull(scores);
        assertEquals(4, scores.size());
        assertEquals(Arrays.asList(10, 20, 30, 40), scores);

        drv.close();
    }

    @Test
    public void testPopFirst() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        String collection = "test_pop_first";

        // Insert test document with array
        InsertMongoCommand insert = new InsertMongoCommand(drv).setDb(db).setColl(collection);
        insert.setDocuments(Arrays.asList(Doc.of("_id", 2, "scores", Arrays.asList(10, 20, 30, 40, 50))));
        insert.execute();

        // Pop first element (-1 means remove first)
        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(collection);
        update.addUpdate(Doc.of("_id", 2), Doc.of("$pop", Doc.of("scores", -1)),
                null, false, false, null, null, null);
        update.execute();

        // Verify
        List<Map<String, Object>> results = drv.find(db, collection, Doc.of("_id", 2), null, null, 0, 0);
        assertEquals(1, results.size());
        List<Integer> scores = (List<Integer>) results.get(0).get("scores");
        assertNotNull(scores);
        assertEquals(4, scores.size());
        assertEquals(Arrays.asList(20, 30, 40, 50), scores);

        drv.close();
    }

    @Test
    public void testPopEmptyArray() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        String collection = "test_pop_empty";

        // Insert test document with empty array
        InsertMongoCommand insert = new InsertMongoCommand(drv).setDb(db).setColl(collection);
        insert.setDocuments(Arrays.asList(Doc.of("_id", 3, "scores", new ArrayList<>())));
        insert.execute();

        // Pop from empty array (should not fail, just do nothing)
        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(collection);
        update.addUpdate(Doc.of("_id", 3), Doc.of("$pop", Doc.of("scores", 1)),
                null, false, false, null, null, null);
        update.execute();

        // Verify array is still empty
        List<Map<String, Object>> results = drv.find(db, collection, Doc.of("_id", 3), null, null, 0, 0);
        assertEquals(1, results.size());
        List<?> scores = (List<?>) results.get(0).get("scores");
        assertNotNull(scores);
        assertEquals(0, scores.size());

        drv.close();
    }

    @Test
    public void testPopMultiple() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        String collection = "test_pop_multiple";

        // Insert test document with array
        InsertMongoCommand insert = new InsertMongoCommand(drv).setDb(db).setColl(collection);
        insert.setDocuments(Arrays.asList(Doc.of("_id", 4, "scores", Arrays.asList(10, 20, 30, 40, 50))));
        insert.execute();

        // Pop last element twice
        UpdateMongoCommand update1 = new UpdateMongoCommand(drv).setDb(db).setColl(collection);
        update1.addUpdate(Doc.of("_id", 4), Doc.of("$pop", Doc.of("scores", 1)),
                null, false, false, null, null, null);
        update1.execute();

        UpdateMongoCommand update2 = new UpdateMongoCommand(drv).setDb(db).setColl(collection);
        update2.addUpdate(Doc.of("_id", 4), Doc.of("$pop", Doc.of("scores", 1)),
                null, false, false, null, null, null);
        update2.execute();

        // Verify
        List<Map<String, Object>> results = drv.find(db, collection, Doc.of("_id", 4), null, null, 0, 0);
        assertEquals(1, results.size());
        List<Integer> scores = (List<Integer>) results.get(0).get("scores");
        assertNotNull(scores);
        assertEquals(3, scores.size());
        assertEquals(Arrays.asList(10, 20, 30), scores);

        drv.close();
    }
}
