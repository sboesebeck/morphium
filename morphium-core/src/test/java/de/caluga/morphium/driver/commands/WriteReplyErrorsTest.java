package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-059: how the reply of an update command is turned into the exception of a rejected write. */
@Tag("core")
public class WriteReplyErrorsTest {

    private static Map<String, Object> error(int code, String msg) {
        return Doc.of("index", 0, "code", code, "errmsg", msg);
    }

    @Test
    public void aCleanReplyIsNotARejection() {
        assertNull(WriteReplyErrors.rejectionOf("db", "coll", null), "w:0 has no reply");
        assertNull(WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0, "n", 1, "nModified", 1)));
        assertNull(WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0, "n", 0, "writeErrors", List.of())), "an empty list is no error");
        assertNull(WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0)));
    }

    @Test
    public void writeErrorsBecomeAnExceptionWithTheCodeOfTheFirstError() {
        MorphiumDriverException e = WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0, "n", 2, "nModified", 1,
                                    "writeErrors", List.of(error(11000, "E11000 duplicate key error collection: db.coll"), error(28, "second"))));
        assertEquals(MorphiumDriverException.class, e.getClass());
        assertEquals(11000, ((Number) e.getMongoCode()).intValue());
        assertTrue(e.isDuplicateKey());
        assertEquals(2, e.getWriteErrors().size());
        assertEquals("coll", e.getCollection());
        assertEquals("db", e.getDb());
        assertTrue(e.getMessage().contains("E11000 duplicate key"), e.getMessage());
        assertTrue(e.getMessage().contains("code 11000"), e.getMessage());
        assertTrue(e.getMessage().contains("matched=2") && e.getMessage().contains("modified=1"), "the counts of the reply: " + e.getMessage());
        assertTrue(e.getMessage().contains("not rolled back"), e.getMessage());
        assertTrue(e.getMessage().contains("2 errors") || e.getMessage().contains("(+1 more)"), "several errors are announced: " + e.getMessage());
    }

    @Test
    public void aWriteConcernErrorIsAnExceptionOfItsOwnTypeAndSaysThatTheWriteWasApplied() {
        MorphiumDriverException e = WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0, "n", 1, "nModified", 1,
                                    "writeConcernError", Doc.of("code", 64, "errmsg", "waiting for replication timed out")));
        assertInstanceOf(WriteConcernNotSatisfiedException.class, e);
        assertEquals(64, ((Number) e.getMongoCode()).intValue());
        assertFalse(e.isDuplicateKey());
        assertTrue(e.getMessage().contains("WAS APPLIED"), e.getMessage());
        assertTrue(e.getMessage().contains("waiting for replication timed out"), e.getMessage());
        assertTrue(e.getMessage().contains("matched=1") && e.getMessage().contains("modified=1"), e.getMessage());
    }

    @Test
    public void writeErrorsWinOverAWriteConcernError() {
        MorphiumDriverException e = WriteReplyErrors.rejectionOf("db", "coll", Doc.of("ok", 1.0, "n", 0,
                                    "writeErrors", List.of(error(11000, "dup")), "writeConcernError", Doc.of("code", 64, "errmsg", "wc")));
        assertFalse(e instanceof WriteConcernNotSatisfiedException, "a rejected statement is reported as rejected");
        assertEquals(11000, ((Number) e.getMongoCode()).intValue());
    }

    @Test
    public void countsAndCodesOfOtherNumberTypesAreRead() {
        MorphiumDriverException e = WriteReplyErrors.rejectionOf("db", "coll", Doc.of("n", 3L, "nModified", 2.0, "writeErrors", List.of(Doc.of("code", 2L, "errmsg", "bad"))));
        assertEquals(2L, e.getMongoCode());
        assertTrue(e.getMessage().contains("matched=3") && e.getMessage().contains("modified=2"), e.getMessage());
    }

    @Test
    public void aMissingCountIsShownAsUnknownNotAsZero() {
        MorphiumDriverException e = WriteReplyErrors.rejectionOf("db", "coll", Doc.of("writeErrors", List.of(error(11000, "dup"))));
        assertTrue(e.getMessage().contains("matched=0"), "the reply of a rejected statement without n: " + e.getMessage());
    }

    @Test
    public void theRejectionIsReturnedNotThrown() {
        // the caller clears caches and fires events before it throws
        assertInstanceOf(MorphiumDriverException.class, WriteReplyErrors.rejectionOf("db", "coll", Doc.of("writeErrors", List.of(error(11000, "dup")))));
    }
}
