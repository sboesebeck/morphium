package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for #252: the wire fast path hardcoded ordered/collation to their defaults
 * instead of reading them from the client's request, so whether a client's `ordered:false` or
 * `collation` was honoured depended on which internal dispatch path a request happened to take.
 *
 * <p>Seam test in the handler's own package - no server or sockets (see CreateIndexesFastPathTest).
 */
public class FastPathOptionsTest {

    private InMemoryDriver drv;
    private final String db = "fastpath_test";
    private final String coll = "docs";

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

    private MongoCommandHandler handler() {
        return new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null);
    }

    private int countAll() throws Exception {
        FindCommand fnd = new FindCommand(drv).setDb(db).setColl(coll);
        fnd.setFilter(Doc.of());
        List<Map<String, Object>> res = fnd.execute();
        fnd.releaseConnection();
        return res.size();
    }

    @Test
    public void insertDirect_honoursOrderedFalse() throws Exception {
        MorphiumId dup = new MorphiumId();
        // Seed the duplicate so the SECOND document of the batch collides.
        List<Map<String, Object>> seed = new ArrayList<>();
        seed.add(Doc.of("_id", dup, "n", 0));
        new de.caluga.morphium.driver.commands.InsertMongoCommand(drv)
            .setDb(db).setColl(coll).setDocuments(seed).execute();

        List<Map<String, Object>> batch = List.of(
            Doc.of("_id", new MorphiumId(), "n", 1),
            Doc.of("_id", dup, "n", 2),                 // duplicate key -> error
            Doc.of("_id", new MorphiumId(), "n", 3));

        handler().processInsertDirect(Doc.of(
            "$db", db, "insert", coll, "documents", batch, "ordered", false));

        // ordered:false must continue past the failing document, so n=1 AND n=3 land (plus the seed).
        assertEquals(3, countAll(),
            "ordered:false must continue past the duplicate and still insert the last document");
    }

    @Test
    public void countDirect_honoursCollation() throws Exception {
        List<Map<String, Object>> seed = new ArrayList<>();
        seed.add(Doc.of("_id", new MorphiumId(), "name", "hello"));
        new de.caluga.morphium.driver.commands.InsertMongoCommand(drv)
            .setDb(db).setColl(coll).setDocuments(seed).execute();

        Map<String, Object> withoutCollation = handler().processCountDirect(Doc.of(
            "$db", db, "count", coll, "query", Doc.of("name", "HELLO")));
        assertEquals(0, ((Number) withoutCollation.get("n")).intValue(),
            "without a collation the comparison is case sensitive");

        Map<String, Object> withCollation = handler().processCountDirect(Doc.of(
            "$db", db, "count", coll, "query", Doc.of("name", "HELLO"),
            "collation", Doc.of("locale", "en", "strength", 1)));
        assertEquals(1, ((Number) withCollation.get("n")).intValue(),
            "a case-insensitive collation from the request must be honoured by the fast path");
    }

    @Test
    public void deleteDirect_honoursCollation() throws Exception {
        List<Map<String, Object>> seed = new ArrayList<>();
        seed.add(Doc.of("_id", new MorphiumId(), "name", "hello"));
        new de.caluga.morphium.driver.commands.InsertMongoCommand(drv)
            .setDb(db).setColl(coll).setDocuments(seed).execute();

        handler().processDeleteDirect(Doc.of(
            "$db", db, "delete", coll,
            "deletes", List.of(Doc.of("q", Doc.of("name", "HELLO"), "limit", 0,
                "collation", Doc.of("locale", "en", "strength", 1)))));

        assertEquals(0, countAll(), "a case-insensitive collation must let the delete match");
    }
}
