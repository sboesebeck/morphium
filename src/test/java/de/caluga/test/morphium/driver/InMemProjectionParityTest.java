package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemProjectionParityTest {

    private InMemoryDriver drv() {
        InMemoryDriver d = new InMemoryDriver();
        d.connect();
        return d;
    }

    @Test
    public void includeNestedDotPath() throws Exception {
        var d = drv();
        String db = "proj_db";
        String coll = "docs";
        new ClearCollectionCommand(d).setDb(db).setColl(coll).doClear();
        ObjectId id = new ObjectId();
        new InsertMongoCommand(d).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", id, "a", Doc.of("b", 1, "c", 2))
        )).execute();

        Map<String, Object> doc = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("a.b", 1))
            .setLimit(1)
            .execute().get(0);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("a"));
        assertThat(((Map)doc.get("a")).keySet()).containsExactly("b");
        assertEquals(1, ((Number)((Map)doc.get("a")).get("b")).intValue());
    }

    @Test
    public void excludeNestedDotPath() throws Exception {
        var d = drv();
        String db = "proj_db";
        String coll = "docs";
        new ClearCollectionCommand(d).setDb(db).setColl(coll).doClear();
        ObjectId id = new ObjectId();
        new InsertMongoCommand(d).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", id, "a", Doc.of("b", 1, "c", 2))
        )).execute();

        Map<String, Object> doc = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("a.b", 0))
            .setLimit(1)
            .execute().get(0);

        assertTrue(doc.containsKey("a"));
        assertFalse(((Map)doc.get("a")).containsKey("b"));
        assertEquals(2, ((Number)((Map)doc.get("a")).get("c")).intValue());
    }

    @Test
    public void sliceProjection() throws Exception {
        var d = drv();
        String db = "proj_db";
        String coll = "docs";
        new ClearCollectionCommand(d).setDb(db).setColl(coll).doClear();
        ObjectId id = new ObjectId();
        new InsertMongoCommand(d).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", id, "arr", List.of(1,2,3,4,5))
        )).execute();

        Map<String, Object> first2 = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("arr", Doc.of("$slice", 2)))
            .execute().get(0);
        assertThat((List) first2.get("arr")).containsExactly(1,2);

        Map<String, Object> last2 = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("arr", Doc.of("$slice", -2)))
            .execute().get(0);
        assertThat((List) last2.get("arr")).containsExactly(4,5);

        Map<String, Object> mid = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("arr", Doc.of("$slice", List.of(1, 3))))
            .execute().get(0);
        assertThat((List) mid.get("arr")).containsExactly(2,3,4);
    }

    @Test
    public void elemMatchProjection() throws Exception {
        var d = drv();
        String db = "proj_db";
        String coll = "docs";
        new ClearCollectionCommand(d).setDb(db).setColl(coll).doClear();
        ObjectId id = new ObjectId();
        new InsertMongoCommand(d).setDb(db).setColl(coll).setDocuments(List.of(
            Doc.of("_id", id, "arr", List.of(Doc.of("x",1), Doc.of("x",3), Doc.of("x",5)))
        )).execute();

        Map<String, Object> doc = new FindCommand(d)
            .setDb(db).setColl(coll)
            .setFilter(Doc.of("_id", id))
            .setProjection(Doc.of("arr", Doc.of("$elemMatch", Doc.of("x", Doc.of("$gt", 2)))))
            .execute().get(0);

        assertTrue(doc.containsKey("arr"));
        List list = (List) doc.get("arr");
        assertThat(list).hasSize(1);
        assertEquals(3, ((Number)((Map)list.get(0)).get("x")).intValue());
    }
}

