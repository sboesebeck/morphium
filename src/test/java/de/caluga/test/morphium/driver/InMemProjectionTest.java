package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemProjectionTest {
    String db = "projdb";
    String coll = "projcoll";

    @Test
    public void includeProjectionKeepsOnlySpecifiedFields() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("a", 1, "b", 2)))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of()).setProjection(Doc.of("a", 1));
        List<Map<String,Object>> res = f.execute();
        assertEquals(1, res.size());
        assertTrue(res.get(0).containsKey("a"));
        assertFalse(res.get(0).containsKey("b"));
        assertTrue(res.get(0).containsKey("_id"));
    }

    @Test
    public void excludeProjectionRemovesSpecifiedFields() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("a", 1, "b", 2)))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of()).setProjection(Doc.of("b", 0));
        List<Map<String,Object>> res = f.execute();
        assertFalse(res.get(0).containsKey("b"));
        assertTrue(res.get(0).containsKey("a"));
    }
}

