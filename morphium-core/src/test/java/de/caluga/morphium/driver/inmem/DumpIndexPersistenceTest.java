package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.json.simple.parser.JSONParser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction and regression tests for #340: dump/restore must carry index definitions.
 *
 * <p>The dump file used to contain exactly the three top-level keys {@code data}, {@code _id},
 * {@code db} - no index information at all. After a FULL cluster restart (every node restores
 * from its own dump, no peer left for an initial sync) the data came back but every index was
 * gone: TTL indexes silently stopped working, every query became a collection scan. A rolling
 * restart hid this, because each restarted node got its indexes back from a running peer via
 * initial sync.
 *
 * <p>The two tests below model exactly that outage case: one driver writes the dump, a FRESH
 * driver (no shared state) restores it.
 *
 * <p>Lives in the driver's own package to reach the package-private {@code runTtlSweepPass()},
 * which makes the TTL sweep deterministic instead of racing the background scheduler (same
 * pattern as {@link TtlQueueInvalidationTest}).
 */
@Tag("inmemory")
public class DumpIndexPersistenceTest {
    private static final String DB = "dump_index_db";
    private static final String COLL = "indexed_coll";

    /**
     * A driver whose background TTL sweep is effectively disabled: {@code expireCheck} is set
     * before {@code connect()} (the period is fixed when the task is scheduled), and the one
     * unconditional tick 100ms after scheduling is waited out. All sweeping is then driven
     * explicitly via {@link InMemoryDriver#runTtlSweepPass()}.
     */
    private InMemoryDriver quiescentDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
        Thread.sleep(400);
        return drv;
    }

    private static Map<String, Object> indexByName(List<Map<String, Object>> indexes, String name) {
        for (Map<String, Object> idx : indexes) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && name.equals(opts.get("name"))) {
                return idx;
            }
        }
        return null;
    }

    /** Writes the given JSON verbatim as a gzip'd dump file - fixtures are LITERALS on purpose:
     * a same-version dump()/restore() roundtrip can never catch the writer silently drifting
     * away from the documented format, a fixed byte sequence can. */
    private static File writeFixture(Path tmp, String fileName, String json) throws Exception {
        File f = new File(tmp.toFile(), fileName);
        try (GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(f))) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return f;
    }

    private static String readDumpJson(File f) throws Exception {
        try (GZIPInputStream gz = new GZIPInputStream(new FileInputStream(f))) {
            return new String(gz.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * The core #340 repro: index definitions must survive a dump/restore across a process
     * boundary (modelled by a second, completely fresh driver).
     */
    @Test
    public void indexDefinitionsSurviveDumpRestoreIntoFreshDriver(@TempDir Path tmp) throws Exception {
        InMemoryDriver src = quiescentDriver();
        try {
            new InsertMongoCommand(src).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(
                        Doc.of("counter", 1, "email", "a@b.c", "expiresAt", new Date()),
                        Doc.of("counter", 2, "email", "d@e.f", "expiresAt", new Date())))
                    .execute();
            src.createIndex(DB, COLL, Doc.of("expiresAt", 1),
                    Doc.of("name", "ttl_expires", "expireAfterSeconds", 3600));
            src.createIndex(DB, COLL, Doc.of("email", 1),
                    Doc.of("name", "uniq_email", "unique", true, "sparse", true));

            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            src.dumpToFile(DB, f);

            InMemoryDriver target = quiescentDriver();
            try {
                target.restoreFromFile(f);

                assertEquals(2, target.getDatabase(DB).get(COLL).size(),
                        "sanity: the documents must be restored");

                List<Map<String, Object>> indexes = target.getIndexes(DB, COLL);
                Map<String, Object> ttl = indexByName(indexes, "ttl_expires");
                Map<String, Object> uniq = indexByName(indexes, "uniq_email");

                assertNotNull(ttl, "#340: the TTL index must survive dump/restore - got only " + indexes);
                assertNotNull(uniq, "#340: the unique index must survive dump/restore - got only " + indexes);

                assertEquals(1, ((Number) ttl.get("expiresAt")).intValue(), "TTL index key spec must survive");
                Map<?, ?> ttlOpts = (Map<?, ?>) ttl.get("$options");
                assertEquals(3600, ((Number) ttlOpts.get("expireAfterSeconds")).intValue(),
                        "expireAfterSeconds must survive");

                Map<?, ?> uniqOpts = (Map<?, ?>) uniq.get("$options");
                assertEquals(Boolean.TRUE, uniqOpts.get("unique"), "unique option must survive");
                assertEquals(Boolean.TRUE, uniqOpts.get("sparse"), "sparse option must survive");
            } finally {
                target.close();
            }
        } finally {
            src.close();
        }
    }

    /**
     * The TTL half of #340, and at the same time the order guard: on restore, indexes are
     * created AFTER {@code setDatabase} on purpose. {@code createIndex} seeds the TTL expiry
     * queue from the documents present at that moment ({@code ttlBootstrapQueue}), and the
     * sweep only re-bootstraps a queue that is {@code null} - never one that merely came up
     * empty. Creating the index BEFORE the data would therefore leave every restored document
     * permanently un-expirable: this test fails in that case, exactly as it fails while #340
     * is unfixed (no index at all).
     */
    @Test
    public void restoredDocumentsExpireViaRestoredTtlIndex(@TempDir Path tmp) throws Exception {
        InMemoryDriver src = quiescentDriver();
        try {
            // already due: expiresAt lies 5s in the past, expireAfterSeconds is 0
            long past = System.currentTimeMillis() - 5_000L;
            new InsertMongoCommand(src).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(
                        Doc.of("counter", 1, "expiresAt", new Date(past)),
                        Doc.of("counter", 2, "expiresAt", new Date(past))))
                    .execute();
            src.createIndex(DB, COLL, Doc.of("expiresAt", 1),
                    Doc.of("name", "ttl_expires", "expireAfterSeconds", 0));

            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            src.dumpToFile(DB, f);

            InMemoryDriver target = quiescentDriver();
            try {
                target.restoreFromFile(f);
                assertEquals(2, target.getDatabase(DB).get(COLL).size(),
                        "sanity: both documents must be restored before the sweep");

                target.runTtlSweepPass();

                assertTrue(target.getDatabase(DB).get(COLL).isEmpty(),
                        "#340: restored, already-due documents must be expired by the restored TTL index - "
                        + "still present: " + target.getDatabase(DB).get(COLL));
            } finally {
                target.close();
            }
        } finally {
            src.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Format fixtures. Dumps outlive the process that wrote them, so both directions of
    // compatibility are pinned here with literal JSON - NOT with dump() output (#340, and the
    // #333 lesson: a write-side format change silently broke every older reader).
    // ---------------------------------------------------------------------------------------

    /** A pre-#340 dump - exactly the three top-level keys data/_id/db - must restore exactly
     * as it always did: documents back, only the lazily seeded _id index, no error. */
    @Test
    public void oldFormatFixtureWithoutIndexesRestoresAsBefore(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"fx_old\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"a\", \"text\" : \"legacy doc\" } ] } }";
        File f = writeFixture(tmp, "fx_old.morphium.gz", json);

        InMemoryDriver target = new InMemoryDriver();
        List<String> failedIndexes = target.restoreInternal(new FileInputStream(f));

        assertTrue(failedIndexes.isEmpty(), "a dump without an indexes section must not report index failures");
        assertEquals(1, target.getDatabase("fx_old").get("coll").size());
        List<Map<String, Object>> indexes = target.getIndexes("fx_old", "coll");
        assertEquals(1, indexes.size(), "an old-format dump must yield only the _id index, got: " + indexes);
        assertNotNull(indexByName(indexes, "_id_1"));
    }

    /**
     * A NEW-format dump as a literal, independent of what dump() currently writes: if the
     * writer ever drifts away from this documented shape, this test stays green while the
     * dump-and-restore roundtrip tests above keep passing too - but then
     * {@link #freshDumpCarriesTheDocumentedShape} fails. Together they pin the format from
     * both sides. Covers every option the driver actually enforces: TTL, unique, sparse,
     * compound with directions, partialFilterExpression.
     */
    @Test
    public void newFormatFixtureRestoresAllIndexOptions(@TempDir Path tmp) throws Exception {
        // expiresAt 1723891234567 = 2024-08-17, always in the past; expireAfterSeconds 0
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"fx_new\", \"data\" : { \"coll\" : [ "
                + "{ \"_id\" : \"due\", \"email\" : \"a@b.c\", \"status\" : \"active\", "
                + "\"expiresAt\" : { \"class_name\" : \"java.util.Date\", \"value\" : 1723891234567 } }, "
                + "{ \"_id\" : \"keeper\", \"email\" : \"d@e.f\", \"status\" : \"done\" } ] }, "
                + "\"indexes\" : { \"coll\" : [ "
                + "{ \"v\" : 2.0, \"key\" : { \"expiresAt\" : 1 }, \"name\" : \"ttl_expires\", \"expireAfterSeconds\" : 0 }, "
                + "{ \"v\" : 2.0, \"key\" : { \"email\" : 1 }, \"name\" : \"uniq_email\", \"unique\" : true, \"sparse\" : true }, "
                + "{ \"v\" : 2.0, \"key\" : { \"status\" : 1, \"counter\" : -1 }, \"name\" : \"status_counter\", "
                + "\"partialFilterExpression\" : { \"status\" : \"active\" } } ] } }";
        File f = writeFixture(tmp, "fx_new.morphium.gz", json);

        InMemoryDriver target = quiescentDriver();
        try {
            List<String> failedIndexes = target.restoreInternal(new FileInputStream(f));
            assertTrue(failedIndexes.isEmpty(), "no index failures expected, got: " + failedIndexes);
            assertEquals(2, target.getDatabase("fx_new").get("coll").size());

            List<Map<String, Object>> indexes = target.getIndexes("fx_new", "coll");

            Map<String, Object> ttl = indexByName(indexes, "ttl_expires");
            assertNotNull(ttl, "TTL index from the fixture must exist, got: " + indexes);
            assertEquals(0, ((Number) ((Map<?, ?>) ttl.get("$options")).get("expireAfterSeconds")).intValue());

            Map<String, Object> uniq = indexByName(indexes, "uniq_email");
            assertNotNull(uniq, "unique index from the fixture must exist");
            assertEquals(Boolean.TRUE, ((Map<?, ?>) uniq.get("$options")).get("unique"));
            assertEquals(Boolean.TRUE, ((Map<?, ?>) uniq.get("$options")).get("sparse"));

            Map<String, Object> compound = indexByName(indexes, "status_counter");
            assertNotNull(compound, "compound index from the fixture must exist");
            assertEquals(1, ((Number) compound.get("status")).intValue());
            assertEquals(-1, ((Number) compound.get("counter")).intValue(), "direction -1 must survive");
            Map<?, ?> pfe = (Map<?, ?>) ((Map<?, ?>) compound.get("$options")).get("partialFilterExpression");
            assertNotNull(pfe, "partialFilterExpression must survive");
            assertEquals("active", pfe.get("status"));

            // the restored TTL index must actually WORK, not just be listed
            target.runTtlSweepPass();
            List<Map<String, Object>> remaining = target.getDatabase("fx_new").get("coll");
            assertEquals(1, remaining.size(), "the due document must be expired, got: " + remaining);
            assertEquals("keeper", remaining.get(0).get("_id"));
        } finally {
            target.close();
        }
    }

    /** Forward compatibility contract, tested from the reader side: unknown top-level keys are
     * ignored. This is exactly the behavior that lets versions without #340 read new dumps -
     * so it must never regress on our side either. */
    @Test
    public void unknownTopLevelKeysAreIgnored(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"fx_future\", "
                + "\"future_field\" : { \"nested\" : [ 1, 2, 3 ] }, \"another\" : 42, "
                + "\"data\" : { \"coll\" : [ { \"_id\" : \"a\" } ] } }";
        File f = writeFixture(tmp, "fx_future.morphium.gz", json);

        InMemoryDriver target = new InMemoryDriver();
        target.restoreFromFile(f);

        assertEquals(1, target.getDatabase("fx_future").get("coll").size(),
                "unknown top-level keys must not break the restore");
    }

    /** The writer side of the format pin: a freshly written dump must carry exactly the
     * documented top-level keys - the three legacy ones untouched, plus indexes only when the
     * database actually has secondary indexes. */
    @Test
    @SuppressWarnings("unchecked")
    public void freshDumpCarriesTheDocumentedShape(@TempDir Path tmp) throws Exception {
        InMemoryDriver src = quiescentDriver();
        try {
            new InsertMongoCommand(src).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("counter", 1))).execute();
            new InsertMongoCommand(src).setDb("plain_db").setColl("plain_coll")
                    .setDocuments(List.of(Doc.of("counter", 1))).execute();
            src.createIndex(DB, COLL, Doc.of("counter", 1), Doc.of("name", "counter_idx"));

            File withIdx = new File(tmp.toFile(), DB + ".morphium.gz");
            src.dumpToFile(DB, withIdx);
            Map<String, Object> root = (Map<String, Object>) new JSONParser().parse(readDumpJson(withIdx));
            assertEquals(Set.of("_id", "db", "data", "indexes"), root.keySet(),
                    "documented dump shape: the three legacy keys plus indexes");
            assertEquals(DB, root.get("db"));
            List<?> docs = (List<?>) ((Map<String, Object>) root.get("data")).get(COLL);
            assertEquals(1, docs.size(), "legacy data key must keep its exact shape");
            List<Map<String, Object>> idxList = (List<Map<String, Object>>)
                    ((Map<String, Object>) root.get("indexes")).get(COLL);
            assertEquals(1, idxList.size(), "_id index must NOT be dumped, only real secondary indexes");
            assertEquals("counter_idx", idxList.get(0).get("name"));
            assertNotNull(idxList.get(0).get("key"), "wire shape: key spec under 'key'");

            // a database without secondary indexes keeps the EXACT pre-#340 shape - no new key
            File plain = new File(tmp.toFile(), "plain_db.morphium.gz");
            src.dumpToFile("plain_db", plain);
            Map<String, Object> plainRoot = (Map<String, Object>) new JSONParser().parse(readDumpJson(plain));
            assertEquals(Set.of("_id", "db", "data"), plainRoot.keySet(),
                    "an index-less dump must stay byte-shape identical to a pre-#340 dump");
        } finally {
            src.close();
        }
    }

    /** Restoring the same dump twice (or into a driver that already holds the same indexes)
     * must not register duplicates - guards the Long-vs-Integer direction normalization: the
     * JSON parser delivers Long(1), the driver holds Integer(1), and createIndex's dedup
     * compares with equals. */
    @Test
    public void restoringTwiceCreatesNoDuplicateIndexes(@TempDir Path tmp) throws Exception {
        InMemoryDriver src = quiescentDriver();
        try {
            new InsertMongoCommand(src).setDb(DB).setColl(COLL)
                    .setDocuments(List.of(Doc.of("email", "a@b.c"))).execute();
            src.createIndex(DB, COLL, Doc.of("email", 1), Doc.of("name", "uniq_email", "unique", true));
            File f = new File(tmp.toFile(), DB + ".morphium.gz");
            src.dumpToFile(DB, f);

            InMemoryDriver target = quiescentDriver();
            try {
                target.restoreFromFile(f);
                assertEquals(2, target.getIndexes(DB, COLL).size(), "_id + uniq_email after first restore");
                target.restoreFromFile(f);
                assertEquals(2, target.getIndexes(DB, COLL).size(),
                        "second restore must not duplicate indexes: " + target.getIndexes(DB, COLL));
            } finally {
                target.close();
            }
        } finally {
            src.close();
        }
    }

    /** A broken index spec must cost neither the data nor the remaining indexes, and must be
     * reported - an index set that LOOKS complete but is not would be worse than none. */
    @Test
    public void brokenIndexSpecDoesNotAbortRestoreAndIsReported(@TempDir Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"fx_broken\", \"data\" : "
                + "{ \"coll\" : [ { \"_id\" : \"a\", \"email\" : \"a@b.c\" } ] }, "
                + "\"indexes\" : { \"coll\" : [ "
                + "{ \"v\" : 2.0, \"key\" : { \"email\" : 1 }, \"name\" : \"good_one\" }, "
                + "{ \"v\" : 2.0, \"key\" : \"NOT_A_DOCUMENT\", \"name\" : \"broken_idx\" }, "
                + "{ \"v\" : 2.0, \"key\" : { \"other\" : -1 }, \"name\" : \"good_two\" } ] } }";
        File f = writeFixture(tmp, "fx_broken.morphium.gz", json);

        InMemoryDriver target = new InMemoryDriver();
        List<String> failedIndexes = target.restoreInternal(new FileInputStream(f));

        assertEquals(1, target.getDatabase("fx_broken").get("coll").size(), "data must be fully restored");
        List<Map<String, Object>> indexes = target.getIndexes("fx_broken", "coll");
        assertNotNull(indexByName(indexes, "good_one"), "index before the broken one must be restored");
        assertNotNull(indexByName(indexes, "good_two"), "index after the broken one must be restored");
        assertNull(indexByName(indexes, "broken_idx"));
        assertEquals(1, failedIndexes.size(), "exactly the broken index must be reported: " + failedIndexes);
        assertTrue(failedIndexes.get(0).contains("fx_broken.coll/broken_idx"),
                "failure entry must name db.coll/index: " + failedIndexes.get(0));
    }
}
