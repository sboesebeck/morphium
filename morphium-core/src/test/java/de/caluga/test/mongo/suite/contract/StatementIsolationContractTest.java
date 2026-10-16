package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.caluga.test.mongo.suite.contract.UpdateContract.canon;
import static de.caluga.test.mongo.suite.contract.UpdateContract.copy;
import static de.caluga.test.mongo.suite.contract.UpdateContract.d;
import static de.caluga.test.mongo.suite.contract.UpdateContract.l;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-028: a driver must not modify the statement it is given, and must not keep references into it. A wire driver cannot do either (it serialises the
 * statement); the InMemoryDriver works on Java objects and has to copy. A caller that reuses an update document for a second call, or changes a list after
 * the write, must not change the stored document or the next write.
 */
@Tag("core")
public class StatementIsolationContractTest extends MultiDriverTestBase {
    private static final String COLL = "isolation_docs";

    private void seed(Morphium m, MongoConnection con, Map<String, Object> doc) throws Exception {
        String db = m.getConfig().connectionSettings().getDatabase();
        try {
            new de.caluga.morphium.driver.commands.DropMongoCommand(con).setDb(db).setColl(COLL).execute();
        } catch (RuntimeException e) {
            // nothing to drop
        }
        new InsertMongoCommand(con).setDb(db).setColl(COLL).setDocuments(new ArrayList<>(List.of(copy(doc)))).execute();
    }

    private Map<String, Object> stored(Morphium m, MongoConnection con) throws Exception {
        String db = m.getConfig().connectionSettings().getDatabase();
        return new de.caluga.morphium.driver.commands.FindCommand(con).setDb(db).setColl(COLL).setFilter(Doc.of()).setProjection(new Doc()).execute().get(0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theUpdateCommandDoesNotModifyTheStatementOrAliasIt(Morphium m) throws Exception {
        try (m) {
            String db = m.getConfig().connectionSettings().getDatabase();
            MongoConnection con = m.getDriver().getPrimaryConnection(null);

            try {
                seed(m, con, d("_id", 1, "n", 1, "a", l(1), "doc", d("x", 1)));
                List<Object> pushed = l(7, 8);
                Map<String, Object> update = d("$inc", d("n", 2), "$push", d("a", d("$each", pushed)), "$set", d("doc", d("y", pushed)));
                String before = canon(update);
                UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(db).setColl(COLL);
                cmd.addUpdate(d("_id", 1), update, null, false, false, null, null, null);
                cmd.execute();
                assertEquals(before, canon(update), "the statement is unchanged");
                pushed.add(99);
                assertEquals(canon(d("_id", 1, "n", 3, "a", l(1, 7, 8), "doc", d("y", l(7, 8)))), canon(stored(m, con)), "a list changed after the write does not reach the stored document");
            } finally {
                m.getDriver().releaseConnection(con);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findAndModifyDoesNotModifyTheStatementOrAliasIt(Morphium m) throws Exception {
        try (m) {
            String db = m.getConfig().connectionSettings().getDatabase();
            MongoConnection con = m.getDriver().getPrimaryConnection(null);

            try {
                seed(m, con, d("_id", 1, "n", 1, "a", l(1), "doc", d("x", 1)));
                List<Object> pushed = l(7, 8);
                Map<String, Object> update = d("$inc", d("n", 2), "$push", d("a", d("$each", pushed)), "$set", d("doc", d("y", pushed)));
                String before = canon(update);
                FindAndModifyMongoCommand cmd = new FindAndModifyMongoCommand(con).setDb(db).setColl(COLL).setQuery(d("_id", 1)).setUpdate(update).setNewFlag(true);
                Map<String, Object> returned = cmd.execute();
                assertEquals(before, canon(update), "the statement is unchanged");
                assertEquals(canon(d("_id", 1, "n", 3, "a", l(1, 7, 8), "doc", d("y", l(7, 8)))), canon(returned), "the returned document");
                pushed.add(99);
                returned.put("n", 1000);
                assertEquals(canon(d("_id", 1, "n", 3, "a", l(1, 7, 8), "doc", d("y", l(7, 8)))), canon(stored(m, con)), "neither a changed list nor a changed result reaches the stored document");
            } finally {
                m.getDriver().releaseConnection(con);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void reusingAnUpdateDocumentGivesTheSameResultTwice(Morphium m) throws Exception {
        try (m) {
            String db = m.getConfig().connectionSettings().getDatabase();
            MongoConnection con = m.getDriver().getPrimaryConnection(null);

            try {
                seed(m, con, d("_id", 1, "n", 1, "a", l(1)));
                Map<String, Object> update = d("$inc", d("n", 2), "$push", d("a", 5));

                for (int i = 0; i < 2; i++) {
                    new FindAndModifyMongoCommand(con).setDb(db).setColl(COLL).setQuery(d("_id", 1)).setUpdate(update).setNewFlag(true).execute();
                }

                assertEquals(canon(d("_id", 1, "n", 5, "a", l(1, 5, 5))), canon(stored(m, con)));
            } finally {
                m.getDriver().releaseConnection(con);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInsertedDocumentIsNotKeptByReference(Morphium m) throws Exception {
        try (m) {
            String db = m.getConfig().connectionSettings().getDatabase();
            MongoConnection con = m.getDriver().getPrimaryConnection(null);

            try {
                Map<String, Object> doc = d("_id", 1, "n", 1, "a", l(1));
                String before = canon(doc);
                new InsertMongoCommand(con).setDb(db).setColl(COLL).setDocuments(new ArrayList<>(List.of(doc))).execute();
                UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(db).setColl(COLL);
                cmd.addUpdate(d("_id", 1), d("$inc", d("n", 2), "$push", d("a", 5)), null, false, false, null, null, null);
                cmd.execute();
                String after = canon(doc);

                if (isInMem(m)) {
                    // pinned deviation MOR-057: the InMemoryDriver stores the inserted map itself, so a later update changes the caller's document
                    assertEquals(canon(d("_id", 1, "n", 3, "a", l(1, 5))), after);
                } else {
                    assertEquals(before, after, "the caller's document is unchanged");
                }

                assertEquals(canon(d("_id", 1, "n", 3, "a", l(1, 5))), canon(stored(m, con)));
            } finally {
                m.getDriver().releaseConnection(con);
            }
        }
    }

    private static boolean isInMem(Morphium m) {
        return UpdateContract.isInMem(m);
    }
}
