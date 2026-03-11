package de.caluga.test.morphium.driver;

import de.caluga.morphium.Collation;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemCollationTest {
    String db = "coldb";
    String coll = "colcoll";

    private Map<String,Object> ci() {
        return Doc.of("locale","en","strength",1); // case-insensitive (primary)
    }

    @Test
    public void findCountUpdateDeleteHonorCollation() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("name","abc"), Doc.of("name","Abc"), Doc.of("name","ABC")))
            .execute();

        // find with collation
        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("name", Doc.of("$in", List.of("abc"))))
            .setCollation(ci());
        var res = f.execute();
        assertEquals(3, res.size());

        // count with collation
        var cnt = new CountMongoCommand(drv).setDb(db).setColl(coll)
            .setQuery(Doc.of("name", Doc.of("$in", List.of("abc"))))
            .setCollation(ci());
        assertEquals(3, cnt.getCount());

        // update with collation (multi)
        new UpdateMongoCommand(drv).setDb(db).setColl(coll)
            .addUpdate(Doc.of("q", Doc.of("name", Doc.of("$in", List.of("abc"))), "u", Doc.of("$set", Doc.of("mark", 1)), "upsert", false, "multi", true, "collation", Doc.of("locale","en","strength",1)))
            .execute();
        var chk = new FindCommand(drv).setDb(db).setColl(coll).setFilter(Doc.of("mark",1)).execute();
        assertEquals(3, chk.size());

        // delete with collation (multi)
        new DeleteMongoCommand(drv).setDb(db).setColl(coll)
            .addDelete(Doc.of("name", Doc.of("$in", List.of("abc"))), 0, Doc.of("locale","en","strength",1), null)
            .execute();
        var left = new FindCommand(drv).setDb(db).setColl(coll).setFilter(Doc.of()).execute();
        assertEquals(0, left.size());
    }
}
