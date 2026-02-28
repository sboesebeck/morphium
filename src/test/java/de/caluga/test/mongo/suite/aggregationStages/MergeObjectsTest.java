package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for $mergeObjects aggregation operator
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@Tag("aggregation")
public class MergeObjectsTest {
    private Morphium morphium;
    private InMemoryDriver drv;
    private static final String db = "test_db";

    @Entity(collectionName = "sales")
    public static class SalesItem {
        @Id
        public MorphiumId id;
        public String item;
        public Map<String, Object> details;
    }

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void testMergeObjectsInGroup() throws Exception {
        String collection = "sales";

        // Insert test documents
        InsertMongoCommand insert = new InsertMongoCommand(drv).setDb(db).setColl(collection);
        insert.setDocuments(Arrays.asList(
            Doc.of("_id", 1, "item", "apple", "details", Doc.of("color", "red", "size", "medium")),
            Doc.of("_id", 2, "item", "apple", "details", Doc.of("weight", 150, "organic", true)),
            Doc.of("_id", 3, "item", "banana", "details", Doc.of("color", "yellow", "size", "large"))
        ));
        insert.execute();

        // Aggregate with $mergeObjects to merge details from all apples
        Aggregator<SalesItem, Map> agg = drv.createAggregator(morphium, SalesItem.class, Map.class);
        agg.setCollectionName(collection);
        agg.addOperator(UtilsMap.of("$match", Doc.of("item", "apple")));
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$item",
            "mergedDetails", Doc.of("$mergeObjects", "$details")
        )));

        List<Map<String, Object>> result = agg.aggregateMap();

        assertEquals(1, result.size());
        Map<String, Object> doc = result.get(0);
        assertEquals("apple", doc.get("_id"));

        Map<String, Object> merged = (Map<String, Object>) doc.get("mergedDetails");
        assertNotNull(merged);
        assertEquals("red", merged.get("color"));
        assertEquals("medium", merged.get("size"));
        assertEquals(150, merged.get("weight"));
        assertEquals(true, merged.get("organic"));
    }
}
