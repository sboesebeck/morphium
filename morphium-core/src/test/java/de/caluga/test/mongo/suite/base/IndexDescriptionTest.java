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
}
