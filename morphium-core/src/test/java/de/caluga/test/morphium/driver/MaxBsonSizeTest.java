package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BSON document size limit, mongod-compatible: inserts/stores beyond maxBsonObjectSize and
 * updates whose RESULT exceeds it are refused with BSONObjectTooLarge (10334) - measured
 * against the real server (8.0.26): a $set/$push growing a document past the limit answers
 * {@code BSONObj size: N (0x..) is invalid. Size must be between 0 and 16793600(16MB)},
 * where 16793600 is the 16MB user limit plus mongod's 16KB internal margin
 * (BSONObjMaxInternalSize). Update results are checked against limit+16KB accordingly,
 * inserts against the plain limit. The limit is configurable per driver
 * (setMaxBsonObjectSize, 0 = unlimited) - tests use small limits so no test needs to
 * allocate real 16MB payloads.
 */
@Tag("inmemory")
public class MaxBsonSizeTest {

    private InMemoryDriver drv;
    private final String db = "bsonsize_test";
    private final String coll = "limit_coll";

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    @Test
    public void defaultLimitIsSixteenMegabytesLikeMongoDB() {
        assertEquals(16 * 1024 * 1024, drv.getMaxBsonObjectSize());
    }

    @Test
    public void insertBeyondTheLimitIsRejectedWithBSONObjectTooLarge() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.insert(db, coll, List.of(Doc.of("_id", 1, "payload", "x".repeat(2048))), null));

        assertEquals(10334, ex.getMongoCode(), "BSONObjectTooLarge: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("BSONObj size:"), ex.getMessage());
        assertEquals(0, drv.count(db, coll, Doc.of(), null, null), "nothing may be stored");
    }

    @Test
    public void insertWithinTheLimitPasses() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        drv.insert(db, coll, List.of(Doc.of("_id", 1, "payload", "x".repeat(100))), null);

        assertEquals(1, drv.count(db, coll, Doc.of(), null, null));
    }

    @Test
    public void unorderedInsertReportsAWriteErrorAndKeepsTheGoodDocuments() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        drv.insert(db, coll, List.of(
                Doc.of("_id", 1, "payload", "x".repeat(2048)),
                Doc.of("_id", 2, "payload", "ok")), null, false);

        assertEquals(1, drv.count(db, coll, Doc.of(), null, null), "the good document must be stored");
        assertEquals(0, drv.count(db, coll, Doc.of("_id", 1), null, null), "the oversized one must not");
    }

    @Test
    public void storeBeyondTheLimitIsRejected() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.store(db, coll, List.of(Doc.of("_id", 1, "payload", "x".repeat(2048))), null));

        assertEquals(10334, ex.getMongoCode());
        assertEquals(0, drv.count(db, coll, Doc.of(), null, null));
    }

    @Test
    public void updateGrowingTheDocumentBeyondTheLimitIsRejectedAtomically() throws Exception {
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "v", "small")), null);
        drv.setMaxBsonObjectSize(1024); // update results are allowed limit + 16KB internal margin

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.update(db, coll, Doc.of("_id", 1), null,
                             Doc.of("$set", Doc.of("payload", "x".repeat(32 * 1024))),
                             false, false, null, null));

        assertEquals(10334, ex.getMongoCode(), "BSONObjectTooLarge: " + ex.getMessage());
        // atomic like mongod: the stored document is untouched
        Map<String, Object> stored = drv.findByFieldValue(db, coll, "_id", 1).get(0);
        assertEquals("small", stored.get("v"));
        assertFalse(stored.containsKey("payload"), "the rejected update must not leave partial state: " + stored.keySet());
    }

    @Test
    public void updateWithinTheInternalMarginPasses() throws Exception {
        drv.insert(db, coll, List.of(Doc.of("_id", 1)), null);
        drv.setMaxBsonObjectSize(1024);

        // beyond the user limit but within limit+16KB - mongod accepts update RESULTS up to
        // BSONObjMaxInternalSize (observed: "Size must be between 0 and 16793600(16MB)")
        drv.update(db, coll, Doc.of("_id", 1), null,
                   Doc.of("$set", Doc.of("payload", "x".repeat(4 * 1024))),
                   false, false, null, null);

        assertEquals(1, drv.count(db, coll, Doc.of("payload", Doc.of("$exists", true)), null, null));
    }

    @Test
    public void replacementBeyondTheLimitIsRejectedAtomically() throws Exception {
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "v", "small")), null);
        drv.setMaxBsonObjectSize(1024);

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.update(db, coll, Doc.of("_id", 1), null,
                             Doc.of("payload", "x".repeat(32 * 1024)),
                             false, false, null, null));

        assertEquals(10334, ex.getMongoCode());
        Map<String, Object> stored = drv.findByFieldValue(db, coll, "_id", 1).get(0);
        assertEquals("small", stored.get("v"), "the replaced document must be rolled back: " + stored.keySet());
    }

    @Test
    public void upsertBeyondTheLimitIsRejected() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.update(db, coll, Doc.of("_id", 42), null,
                             Doc.of("$set", Doc.of("payload", "x".repeat(32 * 1024))),
                             false, true, null, null));

        assertEquals(10334, ex.getMongoCode());
        assertEquals(0, drv.count(db, coll, Doc.of(), null, null), "the upsert must not create the document");
    }

    @Test
    public void zeroDisablesTheLimit() throws Exception {
        drv.setMaxBsonObjectSize(1024);
        Map<String, Object> doc = Doc.of("_id", 1, "payload", "x".repeat(2048));
        assertThrows(MorphiumDriverException.class, () -> drv.insert(db, coll, List.of(doc), null));

        drv.setMaxBsonObjectSize(0);

        drv.insert(db, coll, List.of(doc), null);
        assertEquals(1, drv.count(db, coll, Doc.of(), null, null));
    }

    @Test
    public void errorMessageMatchesTheMongoShape() throws Exception {
        drv.setMaxBsonObjectSize(1024);

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
            () -> drv.insert(db, coll, List.of(Doc.of("_id", 7, "payload", "x".repeat(2048))), null));

        // mongod 8.0.26: "BSONObj size: 18874398 (0x120001E) is invalid. Size must be
        // between 0 and 16793600(16MB) First element: _id: 1"
        assertTrue(ex.getMessage().matches("BSONObj size: \\d+ \\(0x[0-9A-F]+\\) is invalid\\. "
                + "Size must be between 0 and \\d+\\(\\d+MB\\) First element: _id: 7.*"),
            "message must match mongod's shape: " + ex.getMessage());
    }
}
