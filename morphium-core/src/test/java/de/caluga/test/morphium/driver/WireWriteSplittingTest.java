package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end: WriteMongoCommand.execute() must split its payload into several wire
 * messages when the statements would exceed the connection's maxMessageSize - transparent
 * for the caller (summed counters, everything stored). The InMemoryDriver doubles as the
 * MongoConnection here; sendCommand is instrumented to observe what would actually go
 * over the wire per message.
 */
@Tag("inmemory")
public class WireWriteSplittingTest {

    private static final int SMALL_MESSAGE_SIZE = 18 * 1024; // → budget 4096 after envelope slack
    private static final int EXPECTED_BUDGET = 4096;

    private final String db = "wiresplit_test";
    private final String coll = "split_coll";

    private RecordingDriver drv;

    /** Records the per-send payload statements of every write command. */
    private static class RecordingDriver extends InMemoryDriver {
        final List<List<Map<String, Object>>> sentPayloads = new ArrayList<>();

        @Override
        public int getMaxMessageSize() {
            return SMALL_MESSAGE_SIZE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            Map<String, Object> m = cmd.asMap();

            for (String key : new String[] {"documents", "updates", "deletes"}) {
                if (m.get(key) instanceof List) {
                    sentPayloads.add(new ArrayList<>((List<Map<String, Object>>) m.get(key)));
                }
            }

            return super.sendCommand(cmd);
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        drv = new RecordingDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private void assertEverySentMessageFitsTheBudget() {
        assertTrue(drv.sentPayloads.size() > 1, "payload must be split into several messages");

        for (List<Map<String, Object>> payload : drv.sentPayloads) {
            int bytes = payload.stream().mapToInt(BsonEncoder::documentSize).sum();
            assertTrue(bytes <= EXPECTED_BUDGET || payload.size() == 1,
                "one wire message carries " + bytes + " bytes payload - over the budget");
        }
    }

    @Test
    public void oversizedInsertIsSplitTransparently() throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            docs.add(Doc.of("_id", i, "p", "x".repeat(1500)));
        }

        Map<String, Object> result = new InsertMongoCommand(drv)
            .setDb(db).setColl(coll).setDocuments(docs).execute();

        assertEquals(20, ((Number) result.get("n")).intValue(), "summed n over all sub-batches: " + result);
        assertEquals(20, drv.count(db, coll, Doc.of(), null, null), "every document must be stored");
        assertEverySentMessageFitsTheBudget();
    }

    @Test
    public void oversizedUpdateIsSplitTransparently() throws Exception {
        List<Map<String, Object>> seed = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            seed.add(Doc.of("_id", i, "v", "old"));
        }

        drv.insert(db, coll, seed, null);
        drv.sentPayloads.clear();

        UpdateMongoCommand cmd = new UpdateMongoCommand(drv).setDb(db).setColl(coll);

        for (int i = 0; i < 20; i++) {
            cmd.addUpdate(Doc.of("_id", i), Doc.of("$set", Doc.of("v", "x".repeat(1500))),
                          null, false, false, null, null, null);
        }

        Map<String, Object> result = cmd.execute();

        assertEquals(20, ((Number) result.get("n")).intValue(), "summed n over all sub-batches: " + result);
        assertEquals(20, drv.count(db, coll, Doc.of("v", Doc.of("$ne", "old")), null, null),
            "every statement must be applied");
        assertEverySentMessageFitsTheBudget();
    }

    @Test
    public void oversizedDeleteIsSplitTransparently() throws Exception {
        List<Map<String, Object>> seed = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            seed.add(Doc.of("_id", i));
        }

        drv.insert(db, coll, seed, null);
        drv.sentPayloads.clear();

        DeleteMongoCommand cmd = new DeleteMongoCommand(drv).setDb(db).setColl(coll);

        for (int i = 0; i < 20; i++) {
            // query padded with an always-true predicate so 20 statements exceed the budget
            cmd.addDelete(Doc.of("q", Doc.of("_id", i, "pad", Doc.of("$ne", "x".repeat(1500))),
                                 "limit", 1));
        }

        Map<String, Object> result = cmd.execute();

        assertEquals(20, ((Number) result.get("n")).intValue(), "summed n over all sub-batches: " + result);
        assertEquals(0, drv.count(db, coll, Doc.of(), null, null), "every document must be deleted");
        assertEverySentMessageFitsTheBudget();
    }

    @Test
    public void payloadsWithArbitraryJavaObjectsSurviveTheSplitMeasurement() throws Exception {
        // embedded in-memory connections accept any Java object as a value - the byte
        // measurement for splitting must skip what BSON cannot encode, not blow up
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            docs.add(Doc.of("_id", i, "obj", new Object()));
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        assertEquals(5, drv.count(db, coll, Doc.of(), null, null));
    }

    @Test
    public void smallWritesStayOneMessage() throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            docs.add(Doc.of("_id", i, "v", i));
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();

        assertEquals(1, drv.sentPayloads.size(), "a payload within the budget must not be split");
    }
}
