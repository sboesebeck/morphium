package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-010: the {@code lastErrorObject} of a findAndModify reply ({@code updatedExisting}, {@code upserted}) reaches the caller through
 * {@code FindAndModifyMongoCommand.executeReply()} on a real MongoDB and on the InMemoryDriver; {@code execute()} still returns only the value.
 */
@Tag("core")
public class FindAndModifyLastErrorObjectTest extends MultiDriverTestBase {

    private FindAndModifyMongoCommand command(Morphium m, MongoConnection con, String id, boolean upsert, boolean returnNew) {
        return new FindAndModifyMongoCommand(con).setDb(m.getConfig().connectionSettings().getDatabase()).setColl(m.getMapper().getCollectionName(Plain.class))
               .setQuery(Doc.of("_id", id)).setUpdate(Doc.of("$set", Doc.of("payload", "x"))).setUpsert(upsert).setNewFlag(returnNew);
    }

    private Map<String, Object> reply(Morphium m, String id, boolean upsert, boolean returnNew) throws Exception {
        MongoConnection con = m.getDriver().getPrimaryConnection(null);
        FindAndModifyMongoCommand cmd = command(m, con, id, upsert, returnNew);

        try {
            return cmd.executeReply();
        } finally {
            cmd.releaseConnection();
        }
    }

    private Map<String, Object> value(Morphium m, String id, boolean upsert, boolean returnNew) throws Exception {
        MongoConnection con = m.getDriver().getPrimaryConnection(null);
        FindAndModifyMongoCommand cmd = command(m, con, id, upsert, returnNew);

        try {
            return cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> lastErrorObject(Map<String, Object> reply) {
        assertNotNull(reply.get("lastErrorObject"), "the reply carries the lastErrorObject: " + reply);
        return (Map<String, Object>) reply.get("lastErrorObject");
    }

    private void seed(Morphium m, String id) {
        Plain p = new Plain();
        p.id = id;
        m.store(p);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpdateOfAnExistingDocumentIsUpdatedExisting(Morphium m) throws Exception {
        try (m) {
            seed(m, "a");
            Map<String, Object> lastError = lastErrorObject(reply(m, "a", false, true));
            assertEquals(Boolean.TRUE, lastError.get("updatedExisting"));
            assertFalse(lastError.containsKey("upserted"), String.valueOf(lastError));
            assertEquals(1, ((Number) lastError.get("n")).intValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInsertingUpsertReportsTheUpsertedIdWithBothReturnFlags(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> lastError = lastErrorObject(reply(m, "fresh-new", true, true));
            assertEquals(Boolean.FALSE, lastError.get("updatedExisting"));
            assertEquals("fresh-new", lastError.get("upserted"));

            Map<String, Object> before = reply(m, "fresh-old", true, false);
            assertNull(before.get("value"), "an insert has no pre-image");
            assertEquals("fresh-old", lastErrorObject(before).get("upserted"), "the lastErrorObject tells that it was an insert, the value alone cannot");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void noMatchWithoutUpsertIsNeitherUpdatedNorUpserted(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> reply = reply(m, "none", false, true);
            assertNull(reply.get("value"));
            Map<String, Object> lastError = lastErrorObject(reply);
            assertEquals(Boolean.FALSE, lastError.get("updatedExisting"));
            assertFalse(lastError.containsKey("upserted"), String.valueOf(lastError));
            assertEquals(0, ((Number) lastError.get("n")).intValue());
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void executeStillReturnsOnlyTheValue(Morphium m) throws Exception {
        try (m) {
            seed(m, "v");
            Map<String, Object> doc = value(m, "v", false, true);
            assertEquals("v", doc.get("_id"));
            assertEquals("x", doc.get("payload"));
            assertNull(value(m, "nothing", false, true));
            assertNull(value(m, "ins", true, false));
            assertEquals("ins2", value(m, "ins2", true, true).get("_id"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDuplicateKeyIsStillThrown(Morphium m) throws Exception {
        try (m) {
            seed(m, "dup");
            MongoConnection con = m.getDriver().getPrimaryConnection(null);
            FindAndModifyMongoCommand cmd = command(m, con, "dup", true, true).setQuery(Doc.of("_id", "dup", "payload", "someone else"));

            try {
                MorphiumDriverException e = assertThrows(MorphiumDriverException.class, cmd::executeReply);
                assertTrue(e.isDuplicateKey(), String.valueOf(e));
            } finally {
                cmd.releaseConnection();
            }
        }
    }
}
