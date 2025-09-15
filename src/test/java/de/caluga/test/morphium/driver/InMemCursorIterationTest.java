package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemCursorIterationTest {
    String db = "curdb";
    String coll = "curcoll";

    @Test
    public void iterateBatchesWithMorphiumCursor() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        List<Map<String,Object>> docs = new ArrayList<>();
        for (int i=0;i<25;i++) docs.add(Doc.of("i", i));
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        MorphiumCursor cur = drv.initIteration(db, coll, Doc.of(), Doc.of("i",1), null, 0, 25, 10, (ReadPreference) null, null, null);

        int total = 0;
        while (cur != null) {
            for (Map<String,Object> d : cur.getBatch()) total++;
            cur = drv.nextIteration(cur);
        }
        assertEquals(25, total);
    }
}

