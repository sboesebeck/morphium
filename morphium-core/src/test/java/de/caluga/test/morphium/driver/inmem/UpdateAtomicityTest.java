package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-053, MOR-054, MOR-058: a statement that the InMemoryDriver rejects leaves the document as it was,
 * and a multi update keeps the documents it changed before the error, with their change stream events. The error codes themselves are covered by the
 * update contract suite, which compares them with a real MongoDB.
 */
@Tag("inmemory")
public class UpdateAtomicityTest {
    private static final String DB = "update_atomicity";
    private static final String COLL = "docs";

    private InMemoryDriver drv;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        drv.close();
    }

    private Map<String, Object> only() throws Exception {
        List<Map<String, Object>> found = drv.find(DB, COLL, Doc.of(), null, null, 0, 0);
        assertEquals(1, found.size());
        return found.get(0);
    }

    @Test
    public void aFailedStatementRestoresTheDocument() throws Exception {
        drv.insert(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "z", 1, "a", "text", "m", List.of(1)))), null, true);

        // $set is applied before $inc fails on the string
        MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
                () -> drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("z", 5, "n", 1), "$inc", Doc.of("a", 1)), false, false, null, null));
        assertEquals(14, e.getMongoCode());

        assertEquals(Doc.of("_id", 1, "z", 1, "a", "text", "m", List.of(1)), only());
    }

    @Test
    public void aFailedWriteErrorRestoresTheDocumentToo() throws Exception {
        drv.insert(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "z", 1, "a", 7))), null, true);

        // the write error (28) is reported in the result, like the update command does; "z" was set before it
        Map<String, Object> res = drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("z", 5, "a.b", 1)), false, false, null, null);
        assertTrue(res.get("writeErrors") instanceof List<?> errors && errors.size() == 1);

        assertEquals(Doc.of("_id", 1, "z", 1, "a", 7), only());
    }

    @Test
    public void findAndModifyFailsWithTheCodeAndInsertsNothingOnUpsert() throws Exception {
        MorphiumDriverException dollar = assertThrows(MorphiumDriverException.class,
                () -> drv.findAndOneAndUpdate(DB, COLL, Doc.of("_id", 1), Doc.of("$set", Doc.of("a", 1), "$inc", Doc.of("a", 1)), null, null, true, true));
        assertEquals(40, dollar.getMongoCode());
        assertEquals(0, drv.find(DB, COLL, Doc.of(), null, null, 0, 0).size());

        drv.insert(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "a", 1))), null, true);
        MorphiumDriverException scalar = assertThrows(MorphiumDriverException.class,
                () -> drv.findAndOneAndUpdate(DB, COLL, Doc.of("_id", 1), Doc.of("$set", Doc.of("x", 1, "a.b", 2)), null, null, false, true));
        assertEquals(28, scalar.getMongoCode());
        assertEquals(Doc.of("_id", 1, "a", 1), only());
    }

    @Test
    public void aFailingMultiUpdateKeepsAndAnnouncesTheDocumentsBeforeTheError() throws Exception {
        drv.insert(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "v", 1), Doc.of("_id", 2, "v", "x"), Doc.of("_id", 3, "v", 1))), null, true);

        MongoConnection con = drv.getPrimaryConnection(null);
        CountDownLatch event = new CountDownLatch(1);
        WatchCommand watch = new WatchCommand(con).setDb(DB).setColl(COLL).setBatchSize(1).setMaxTimeMS(5000).setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                event.countDown();
            }

            @Override
            public boolean isContinued() {
                return event.getCount() > 0;
            }
        });
        Thread watcher = Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                watch.releaseConnection();
            }
        });
        Thread.sleep(100); // the watch registers before the write, like ChangeStreamInMemTest

        MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
                () -> drv.update(DB, COLL, Doc.of(), null, Doc.of("$inc", Doc.of("v", 1)), true, false, null, null));
        assertEquals(14, e.getMongoCode());

        assertTrue(event.await(5, TimeUnit.SECONDS), "the change stream must see the document that was changed before the error");
        watcher.join();
        List<Map<String, Object>> found = drv.find(DB, COLL, Doc.of(), Doc.of("_id", 1), null, 0, 0);
        assertEquals(2, found.get(0).get("v"));
        assertEquals("x", found.get(1).get("v"));
        assertEquals(1, found.get(2).get("v"));
    }
}
