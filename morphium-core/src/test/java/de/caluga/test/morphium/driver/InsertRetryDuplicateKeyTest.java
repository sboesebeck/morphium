package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #359: a write whose reply is lost is re-sent by
 * {@link de.caluga.morphium.driver.commands.WriteMongoCommand}. Since Morphium assigns the
 * {@code _id} on the client, that retry collides with its own first attempt and mongod answers
 * with E11000 on the {@code _id} index - a write that succeeded reported as a failure.
 *
 * Purely in-memory, no MongoDB required.
 */
@Tag("inmemory")
class InsertRetryDuplicateKeyTest {

    /** Connection that answers a scripted sequence; a null entry means "reply lost". */
    static class ScriptedConnection extends ConnectionMock {
        private final Deque<Map<String, Object>> answers = new ArrayDeque<>();
        private final boolean[] hasAnswer;
        private int read = 0;
        private MorphiumDriver driver;

        ScriptedConnection(List<Map<String, Object>> scripted) {
            hasAnswer = new boolean[scripted.size()];
            for (int i = 0; i < scripted.size(); i++) {
                hasAnswer[i] = scripted.get(i) != null;
                if (scripted.get(i) != null) {
                    answers.add(scripted.get(i));
                }
            }
        }

        void setDriverMock(MorphiumDriver d) {
            this.driver = d;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConnectedTo() {
            return "localhost:27017";
        }

        @Override
        public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd) {
            return 1;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) {
            int idx = read++;
            return idx < hasAnswer.length && hasAnswer[idx] ? answers.poll() : null;
        }

        @Override
        public MorphiumDriver getDriver() {
            return driver;
        }

        int getReadCallCount() {
            return read;
        }
    }

    static class FastDriverMock extends DriverMock {
        private final MongoConnection con;

        FastDriverMock(MongoConnection con) {
            this.con = con;
        }

        @Override
        public int getSleepBetweenErrorRetries() {
            return 1;
        }

        @Override
        public int getRetriesOnNetworkError() {
            return 1;
        }

        @Override
        public MongoConnection getPrimaryConnection(de.caluga.morphium.driver.WriteConcern wc) {
            return con;
        }
    }

    private static Map<String, Object> duplicateKeyAnswer(Map<String, Object> keyPattern, Map<String, Object> keyValue,
            String errmsg) {
        return Doc.of("ok", 1.0, "n", 0, "writeErrors",
                List.of(Doc.of("index", 0, "code", 11000, "keyPattern", keyPattern, "keyValue", keyValue,
                        "errmsg", errmsg)));
    }

    private static InsertMongoCommand insertOf(ScriptedConnection con, Map<String, Object> doc) {
        FastDriverMock drv = new FastDriverMock(con);
        con.setDriverMock(drv);
        return new InsertMongoCommand(con).setDocuments(List.of(doc)).setDb("test_db").setColl("test_coll");
    }

    /**
     * The bug: first attempt commits but its reply is lost, the retry hits E11000 on our own
     * _id. The document IS stored, so this has to be reported as a successful write.
     */
    @Test
    void retryAfterLostReplyTreatsOwnDuplicateIdAsSuccess() throws MorphiumDriverException {
        MorphiumId id = new MorphiumId();
        Map<String, Object> doc = Doc.of("_id", id, "value", "v");
        ScriptedConnection con = new ScriptedConnection(java.util.Arrays.asList(
                null,
                duplicateKeyAnswer(Doc.of("_id", 1), Doc.of("_id", id),
                        "E11000 duplicate key error collection: test_db.test_coll index: _id_ dup key: { _id: ObjectId('"
                                + id + "') }")));

        Map<String, Object> result = insertOf(con, doc).execute();

        assertEquals(2, con.getReadCallCount(), "the lost reply must have been retried exactly once");
        assertFalse(result.containsKey("writeErrors"),
                "our own _id collision on the retry is not a write error");
        assertEquals(1, ((Number) result.get("n")).intValue(), "the document was written by the first attempt");
    }

    /**
     * A collision on any other unique index is a genuine conflict, even on a retry - nothing
     * says our own first attempt caused it.
     */
    @Test
    void retryKeepsDuplicateKeyOnAnotherUniqueIndexAsError() {
        MorphiumId id = new MorphiumId();
        Map<String, Object> doc = Doc.of("_id", id, "email", "a@b.c");
        ScriptedConnection con = new ScriptedConnection(java.util.Arrays.asList(
                null,
                duplicateKeyAnswer(Doc.of("email", 1), Doc.of("email", "a@b.c"),
                        "E11000 duplicate key error collection: test_db.test_coll index: email_1 dup key: { email: \"a@b.c\" }")));

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, () -> insertOf(con, doc).execute());
        assertTrue(thrown.getMessage().contains("11000"), "the duplicate key error must still surface");
    }

    /** Without a preceding retry there is no first attempt of ours - the collision is real. */
    @Test
    void firstAttemptDuplicateIdStaysAnError() {
        MorphiumId id = new MorphiumId();
        Map<String, Object> doc = Doc.of("_id", id, "value", "v");
        ScriptedConnection con = new ScriptedConnection(List.of(
                duplicateKeyAnswer(Doc.of("_id", 1), Doc.of("_id", id),
                        "E11000 duplicate key error collection: test_db.test_coll index: _id_ dup key: { _id: ObjectId('"
                                + id + "') }")));

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, () -> insertOf(con, doc).execute());
        assertTrue(thrown.getMessage().contains("11000"), "a first-attempt duplicate _id is a real error");
        assertEquals(1, con.getReadCallCount(), "no retry involved");
    }

    /**
     * A batch where only one document collides with its own retried _id: that one counts as
     * written, a genuine error on another document still surfaces.
     */
    @Test
    void retryReconcilesOnlyTheOwnIdCollisionInABatch() {
        MorphiumId id1 = new MorphiumId();
        MorphiumId id2 = new MorphiumId();
        FastDriverMock[] holder = new FastDriverMock[1];
        ScriptedConnection con = new ScriptedConnection(java.util.Arrays.asList(
                null,
                Doc.of("ok", 1.0, "n", 0, "writeErrors", List.of(
                        Doc.of("index", 0, "code", 11000, "keyPattern", Doc.of("_id", 1), "keyValue", Doc.of("_id", id1),
                                "errmsg", "E11000 duplicate key error collection: test_db.test_coll index: _id_"),
                        Doc.of("index", 1, "code", 121, "errmsg", "Document failed validation")))));
        holder[0] = new FastDriverMock(con);
        con.setDriverMock(holder[0]);
        InsertMongoCommand cmd = new InsertMongoCommand(con)
                .setDocuments(List.of(Doc.of("_id", id1, "value", "a"), Doc.of("_id", id2, "value", "b")))
                .setDb("test_db").setColl("test_coll").setOrdered(false);

        MorphiumDriverException thrown = assertThrows(MorphiumDriverException.class, cmd::execute);
        assertTrue(thrown.getMessage().contains("121"), "the validation failure must still surface");
        assertFalse(thrown.getMessage().contains("11000"), "the own-_id collision must have been reconciled");
        assertEquals(1, thrown.getWriteErrors().size(), "only the genuine error is left");
    }
}
