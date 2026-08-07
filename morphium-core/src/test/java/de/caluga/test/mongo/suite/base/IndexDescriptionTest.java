package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;


@Tag("core")
public class IndexDescriptionTest {

    @Test
    public void asMapTest() throws Exception {
        IndexDescription idx = new IndexDescription();
        idx.setBits(12).setKey(Doc.of("_id", 1));
        idx.setName("Test");

        var m = idx.asMap();
        assertTrue(m.containsKey("key"));
        assertTrue(m.containsKey("bits"));
        assertTrue(m.containsKey("name"));
        assertEquals("Test", m.get("name"));
    }

    @Test
    public void asMapFromMapTest() throws Exception {
        IndexDescription idx = new IndexDescription();
        idx.setBits(12).setKey(Doc.of("_id", 1, "value1", -1));
        idx.setName("Test").setBackground(true).setHidden(false).setMax(10).setMin(4)
           .setExpireAfterSeconds(1000)
           .setUnique(true)
           .setWildcardProjection(Doc.of("$**", 1))
           .setSparse(true)
           .setTextIndexVersion(42);

        var m = idx.asMap();
        var idx2 = IndexDescription.fromMap(m);
        assertEquals(idx.getName(), idx2.getName());
        assertEquals(idx.getBits(), idx2.getBits());
        assertEquals(idx.getCollation(), idx2.getCollation());
        assertEquals(idx.getKey(), idx2.getKey());
        assertEquals(idx.getHidden(), idx2.getHidden());
        assertEquals(idx.getSparse(), idx2.getSparse());
    }

    // Regression test: fromMap() used to append a trailing "_" separator after every key
    // instead of only BETWEEN keys, producing names like "campaignNumber_1_" instead of the
    // MongoDB-standard "campaignNumber_1". That mismatch breaks index creation on any database
    // where the correctly-named index already exists (MongoDB rejects it with "Error 85 - Index
    // already exists with a different name", which Morphium only logs as a warning). Neither
    // pre-existing test above catches this: both set an explicit name, which skips the
    // auto-naming branch entirely.
    @Test
    public void fromMap_singleField_generatesNameWithoutTrailingUnderscore() throws Exception {
        var idx = IndexDescription.fromMaps(Doc.of("campaignNumber", 1), null);
        assertEquals("campaignNumber_1", idx.getName());
    }

    @Test
    public void fromMap_multiField_generatesNameJoinedByUnderscoreWithoutTrailingUnderscore() throws Exception {
        var idx = IndexDescription.fromMaps(Doc.of("campaignNumber", 1, "fileName", 1), null);
        assertEquals("campaignNumber_1_fileName_1", idx.getName());
    }

    @Test
    public void fromMap_explicitName_isNotOverwritten() throws Exception {
        var idx = IndexDescription.fromMaps(Doc.of("campaignNumber", 1),
                Doc.of("name", "myCustomIndexName"));
        assertEquals("myCustomIndexName", idx.getName());
    }
}
