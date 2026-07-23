package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #257: command-surface fixes for the in-memory server.
 *
 * currentOp used to answer {ok:0.0, errmsg:"no running ops in memory"} - monitoring tooling reads
 * ok:0 as "server is failing", not "idle". mongod's shape is {inprog:[...], ok:1}. serverStatus and
 * bulkWrite were entirely unregistered ("Unknown command"). bulkWrite is implemented in the
 * MongoDB 8.0 wire shape (ops referencing nsInfo) and fans out to the existing single-op write
 * paths; serverStatus answers the minimal document monitoring tools commonly read.
 */
@Tag("inmemory")
public class InMemServerCommandsTest {

    private InMemoryDriver drv;
    private final String db = "server_cmds_test";
    private final String coll = "bw_coll";

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

    private Map<String, Object> run(Map<String, Object> cmdMap) throws Exception {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(cmdMap);
        int id = drv.runCommand(cmd);
        assertTrue(id > 0, "command must queue a result");
        Map<String, Object> res = drv.readSingleAnswer(id);
        assertNotNull(res);
        return res;
    }

    private void insert(String collection, Map<String, Object>... docs) throws Exception {
        new InsertMongoCommand(drv).setDb(db).setColl(collection).setDocuments(List.of(docs)).execute();
    }

    private List<Map<String, Object>> findAll(String collection) throws Exception {
        FindCommand fnd = new FindCommand(drv).setDb(db).setColl(collection).setFilter(Doc.of());
        List<Map<String, Object>> res = fnd.execute();
        fnd.releaseConnection();
        return res;
    }

    // ---- dbStats / collStats -----------------------------------------------------------

    @Test
    public void dbStats_reportsRealSizes() throws Exception {
        insert(coll, Doc.of("name", "one", "payload", "x".repeat(100)),
               Doc.of("name", "two", "payload", "y".repeat(100)));

        Map<String, Object> res = run(Doc.of("dbStats", 1, "$db", db));

        assertEquals(1.0, res.get("ok"));
        assertEquals(1L, ((Number) res.get("collections")).longValue());
        assertEquals(2L, ((Number) res.get("objects")).longValue());

        long expected = 0;
        for (Map<String, Object> doc : findAll(coll)) {
            expected += BsonEncoder.encodeDocument(doc).length;
        }
        assertEquals(expected, ((Number) res.get("dataSize")).longValue(),
            "dataSize must be the real BSON size of all documents: " + res);
        assertEquals(expected, ((Number) res.get("storageSize")).longValue(),
            "no padding/compression in memory - storageSize equals dataSize");
        assertEquals(expected / 2.0, ((Number) res.get("avgObjSize")).doubleValue(), 0.001);
        assertTrue(((Number) res.get("indexes")).longValue() >= 1, "at least the _id index: " + res);
        assertTrue(((Number) res.get("indexSize")).longValue() > 0, "estimated index size: " + res);
        assertEquals(expected + ((Number) res.get("indexSize")).longValue(),
            ((Number) res.get("totalSize")).longValue());
        assertTrue(((Number) res.get("fsUsedSize")).longValue() > 0, "heap used: " + res);
        assertTrue(((Number) res.get("fsTotalSize")).longValue() >= ((Number) res.get("fsUsedSize")).longValue());
    }

    @Test
    public void collStats_reportsRealSizes() throws Exception {
        insert(coll, Doc.of("name", "one", "payload", "x".repeat(50)));

        Map<String, Object> res = run(Doc.of("collStats", coll, "$db", db));

        assertEquals(1.0, res.get("ok"));
        assertEquals(db + "." + coll, res.get("ns"));
        assertEquals(1L, ((Number) res.get("count")).longValue());
        long expected = BsonEncoder.encodeDocument(findAll(coll).get(0)).length;
        assertEquals(expected, ((Number) res.get("size")).longValue(),
            "size must be the real BSON size, not a shallow ArrayList header: " + res);
        assertEquals(expected, ((Number) res.get("storageSize")).longValue());
        assertEquals(expected, ((Number) res.get("avgObjSize")).longValue());
        assertTrue(((Number) res.get("nindexes")).intValue() >= 1, "at least the _id index: " + res);
        assertTrue(((Number) res.get("totalIndexSize")).longValue() > 0, "estimated index size: " + res);
        assertEquals(expected + ((Number) res.get("totalIndexSize")).longValue(),
            ((Number) res.get("totalSize")).longValue());
    }

    @Test
    public void collStats_unknownCollectionAnswersZeros() throws Exception {
        Map<String, Object> res = run(Doc.of("collStats", "does_not_exist", "$db", "no_such_db"));

        assertEquals(1.0, res.get("ok"), "stats on a missing collection must not fail: " + res);
        assertEquals(0L, ((Number) res.get("count")).longValue());
        assertEquals(0L, ((Number) res.get("size")).longValue());
        assertEquals(0L, ((Number) res.get("totalIndexSize")).longValue());
    }

    // ---- currentOp -------------------------------------------------------------------

    @Test
    public void currentOp_idleAnswersOkWithEmptyInprog() throws Exception {
        Map<String, Object> res = run(Doc.of("currentOp", 1, "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "idle is not a failure - mongod answers ok:1, got: " + res);
        assertInstanceOf(List.class, res.get("inprog"), "mongod shape carries an inprog array");
        assertTrue(((List<?>) res.get("inprog")).isEmpty(), "no in-flight ops in the in-memory server");
        assertFalse(res.containsKey("errmsg"), "a successful answer must not carry errmsg");
    }

    @Test
    public void currentOp_withSecsRunningFilterStillParses() throws Exception {
        // the optional filter shape mongosh sends: {currentOp:1, secs_running:{$gt:n}}
        Map<String, Object> res = run(Doc.of("currentOp", 1, "secs_running", Doc.of("$gt", 5), "$db", "admin"));

        assertEquals(1.0, res.get("ok"));
        assertInstanceOf(List.class, res.get("inprog"));
    }

    // ---- serverStatus ----------------------------------------------------------------

    @Test
    public void serverStatus_minimalDocumentForMonitoringTools() throws Exception {
        Map<String, Object> res = run(Doc.of("serverStatus", 1, "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "serverStatus must succeed, got: " + res);
        assertInstanceOf(String.class, res.get("host"));
        assertFalse(((String) res.get("host")).isBlank());
        assertInstanceOf(String.class, res.get("version"));
        assertFalse(((String) res.get("version")).isBlank());
        assertInstanceOf(String.class, res.get("process"));
        assertInstanceOf(Number.class, res.get("pid"));
        assertInstanceOf(Number.class, res.get("uptime"));
        assertTrue(((Number) res.get("uptime")).doubleValue() >= 0.0);
        assertInstanceOf(Number.class, res.get("uptimeMillis"));
        assertInstanceOf(Date.class, res.get("localTime"));

        Map<String, Object> connections = (Map<String, Object>) res.get("connections");
        assertNotNull(connections, "monitoring dashboards read connections{}");
        assertInstanceOf(Number.class, connections.get("current"));
        assertInstanceOf(Number.class, connections.get("available"));

        assertInstanceOf(Map.class, res.get("mem"), "monitoring dashboards read mem{}");
    }

    // ---- bulkWrite (MongoDB 8.0 top-level command shape) -----------------------------

    @Test
    public void bulkWrite_insertOpsAreApplied() throws Exception {
        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(
                Doc.of("insert", 0, "document", Doc.of("_id", "a", "v", 1)),
                Doc.of("insert", 0, "document", Doc.of("_id", "b", "v", 2))),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "got: " + res);
        assertEquals(2, ((Number) res.get("nInserted")).intValue());
        assertEquals(0, ((Number) res.get("nErrors")).intValue());

        Map<String, Object> cursor = (Map<String, Object>) res.get("cursor");
        assertNotNull(cursor, "mongod 8.0 returns per-op results in a cursor");
        List<Map<String, Object>> batch = (List<Map<String, Object>>) cursor.get("firstBatch");
        assertEquals(2, batch.size());
        assertEquals(1.0, batch.get(0).get("ok"));
        assertEquals(0, ((Number) batch.get(0).get("idx")).intValue());
        assertEquals(1, ((Number) batch.get(0).get("n")).intValue());
        assertEquals(1, ((Number) batch.get(1).get("idx")).intValue());

        assertEquals(2, findAll(coll).size(), "both documents must actually be inserted");
    }

    @Test
    public void bulkWrite_updateAndDeleteOpsAcrossNamespaces() throws Exception {
        insert(coll, Doc.of("_id", "u1", "v", 1), Doc.of("_id", "u2", "v", 1));
        insert("bw_other", Doc.of("_id", "d1", "v", 1), Doc.of("_id", "d2", "v", 1));

        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(
                Doc.of("update", 0, "filter", Doc.of("_id", "u1"), "updateMods", Doc.of("$set", Doc.of("v", 42))),
                Doc.of("delete", 1, "filter", Doc.of(), "multi", true)),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll), Doc.of("ns", db + ".bw_other")),
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "got: " + res);
        assertEquals(1, ((Number) res.get("nMatched")).intValue());
        assertEquals(1, ((Number) res.get("nModified")).intValue());
        assertEquals(2, ((Number) res.get("nDeleted")).intValue());
        assertEquals(0, ((Number) res.get("nErrors")).intValue());

        List<Map<String, Object>> batch =
            (List<Map<String, Object>>) ((Map<String, Object>) res.get("cursor")).get("firstBatch");
        assertEquals(2, batch.size());
        assertEquals(1, ((Number) batch.get(0).get("nModified")).intValue(), "update op result carries nModified");
        assertEquals(2, ((Number) batch.get(1).get("n")).intValue(), "delete op result carries n");

        Map<String, Object> updated = findAll(coll).stream()
            .filter(d -> "u1".equals(d.get("_id"))).findFirst().orElseThrow();
        assertEquals(42, updated.get("v"));
        assertTrue(findAll("bw_other").isEmpty(), "multi delete must remove all documents");
    }

    @Test
    public void bulkWrite_upsertIsCountedAndReported() throws Exception {
        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(Doc.of("update", 0, "filter", Doc.of("_id", "ups1"),
                "updateMods", Doc.of("$set", Doc.of("v", 7)), "upsert", true)),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "got: " + res);
        assertEquals(1, ((Number) res.get("nUpserted")).intValue());
        assertEquals(0, ((Number) res.get("nMatched")).intValue(), "an upsert-insert did not match anything");

        List<Map<String, Object>> batch =
            (List<Map<String, Object>>) ((Map<String, Object>) res.get("cursor")).get("firstBatch");
        assertEquals(1, ((Number) batch.get(0).get("n")).intValue(), "per-op n includes the upserted document");
        Map<String, Object> upserted = (Map<String, Object>) batch.get(0).get("upserted");
        assertNotNull(upserted, "upsert op result must report the upserted _id");
        assertEquals("ups1", upserted.get("_id"));

        assertEquals(1, findAll(coll).size());
    }

    @Test
    public void bulkWrite_orderedStopsAtFirstError() throws Exception {
        insert(coll, Doc.of("_id", "dup", "v", 0));

        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(
                Doc.of("insert", 0, "document", Doc.of("_id", "dup", "v", 1)),
                Doc.of("insert", 0, "document", Doc.of("_id", "fresh", "v", 2))),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "ordered", true,
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "per-op write errors are not a command failure, got: " + res);
        assertEquals(1, ((Number) res.get("nErrors")).intValue());
        assertEquals(0, ((Number) res.get("nInserted")).intValue());

        List<Map<String, Object>> batch =
            (List<Map<String, Object>>) ((Map<String, Object>) res.get("cursor")).get("firstBatch");
        assertEquals(1, batch.size(), "ordered:true must not execute ops after the first error");
        assertEquals(0.0, batch.get(0).get("ok"));
        assertEquals(0, ((Number) batch.get(0).get("idx")).intValue());
        assertEquals(11000, ((Number) batch.get(0).get("code")).intValue());
        assertNotNull(batch.get(0).get("errmsg"));

        assertEquals(1, findAll(coll).size(), "the op after the error must not have been applied");
    }

    @Test
    public void bulkWrite_unorderedContinuesAfterError() throws Exception {
        insert(coll, Doc.of("_id", "dup", "v", 0));

        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(
                Doc.of("insert", 0, "document", Doc.of("_id", "dup", "v", 1)),
                Doc.of("insert", 0, "document", Doc.of("_id", "fresh", "v", 2))),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "ordered", false,
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "got: " + res);
        assertEquals(1, ((Number) res.get("nErrors")).intValue());
        assertEquals(1, ((Number) res.get("nInserted")).intValue());

        List<Map<String, Object>> batch =
            (List<Map<String, Object>>) ((Map<String, Object>) res.get("cursor")).get("firstBatch");
        assertEquals(2, batch.size());
        assertEquals(0.0, batch.get(0).get("ok"));
        assertEquals(1.0, batch.get(1).get("ok"));
        assertEquals(1, ((Number) batch.get(1).get("idx")).intValue());

        assertEquals(2, findAll(coll).size(), "the second insert must have been applied");
    }

    @Test
    public void bulkWrite_errorsOnlyReturnsOnlyFailedOps() throws Exception {
        insert(coll, Doc.of("_id", "dup", "v", 0));

        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(
                Doc.of("insert", 0, "document", Doc.of("_id", "fresh", "v", 1)),
                Doc.of("insert", 0, "document", Doc.of("_id", "dup", "v", 2))),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "ordered", false,
            "errorsOnly", true,
            "$db", "admin"));

        assertEquals(1.0, res.get("ok"), "got: " + res);
        List<Map<String, Object>> batch =
            (List<Map<String, Object>>) ((Map<String, Object>) res.get("cursor")).get("firstBatch");
        assertEquals(1, batch.size(), "errorsOnly must suppress successful per-op results");
        assertEquals(0.0, batch.get(0).get("ok"));
        assertEquals(1, ((Number) batch.get(0).get("idx")).intValue());
    }

    @Test
    public void bulkWrite_invalidNsIndexFailsTheCommand() throws Exception {
        Map<String, Object> res = run(Doc.of(
            "bulkWrite", 1,
            "ops", List.of(Doc.of("insert", 5, "document", Doc.of("_id", "x"))),
            "nsInfo", List.of(Doc.of("ns", db + "." + coll)),
            "$db", "admin"));

        assertEquals(0.0, res.get("ok"), "a structurally broken request fails the whole command");
        assertNotNull(res.get("errmsg"));
    }

    @Test
    public void bulkWrite_missingOpsFailsTheCommand() throws Exception {
        Map<String, Object> res = run(Doc.of("bulkWrite", 1, "$db", "admin"));

        assertEquals(0.0, res.get("ok"));
        assertNotNull(res.get("errmsg"));
    }
}
