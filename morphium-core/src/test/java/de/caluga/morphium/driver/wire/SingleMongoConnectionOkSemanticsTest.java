package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * How a reply's {@code ok} field is judged on the wire - the half of the bug that
 * {@code CommandStatusTest} pins at helper level, here through the real read paths.
 *
 * <p>Background: servers encode {@code ok} as a double ({@code 1.0}/{@code 0.0}) on most paths
 * and as int32 on others, PoppyDB's own error paths included. Literal comparisons got both
 * directions wrong in this codebase: {@code readSingleAnswer()} applied {@code ok == 0.0} to the
 * <em>single-answer</em> path, so an int32 {@code ok: 0} - an error reply - counted as success
 * (write errors were silently swallowed by {@code MorphiumWriterImpl}); and
 * {@code checkForError()} applied {@code ok != 1.0} to the <em>read</em> path, so an int32
 * {@code ok: 1} - a perfectly good reply - was reported as {@code "Error: 1 - null"}.
 */
@Tag("core")
public class SingleMongoConnectionOkSemanticsTest {

    private static final int REQUEST_ID = 4711;

    private ServerSocket serverSocket;
    private Socket serverSide;
    private Socket clientSide;

    private SingleMongoConnection connect() throws Exception {
        serverSocket = new ServerSocket(0);
        clientSide = new Socket("127.0.0.1", serverSocket.getLocalPort());
        serverSide = serverSocket.accept();

        PooledDriver driver = new PooledDriver();
        driver.setMaxWaitTime(2000);

        SingleMongoConnection con = new SingleMongoConnection();
        set(con, "s", clientSide);
        set(con, "in", clientSide.getInputStream());
        set(con, "out", clientSide.getOutputStream());
        set(con, "connected", true);
        set(con, "connectedTo", "localhost-test");
        set(con, "driver", driver);
        return con;
    }

    private static void set(SingleMongoConnection con, String field, Object value) throws Exception {
        Field f = SingleMongoConnection.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(con, value);
    }

    /** Writes one reply for {@link #REQUEST_ID} - the connection reads whatever is on the socket. */
    private void writeReply(Map<String, Object> doc) throws Exception {
        OpMsg resp = new OpMsg();
        resp.setMessageId(4712);
        resp.setResponseTo(REQUEST_ID);
        resp.setFirstDoc(doc);
        serverSide.getOutputStream().write(resp.bytes());
        serverSide.getOutputStream().flush();
    }

    private static Map<String, Object> cursorReply(Object ok) {
        return Doc.of("ok", ok, "cursor", Doc.of("id", 0L, "ns", "test.coll",
                       "firstBatch", List.of(Doc.of("x", 1))));
    }

    @AfterEach
    public void tearDown() throws Exception {
        for (AutoCloseable c : new AutoCloseable[] {serverSide, clientSide, serverSocket}) {
            if (c != null) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Test
    public void integerZeroOkIsAnErrorOnTheSingleAnswerPath() throws Exception {
        SingleMongoConnection con = connect();
        // the shrinking case: an int32 ok:0 error reply used to read as success (equals(0.0) is
        // false for an Integer) - MorphiumWriterImpl then treated a failed update as done
        writeReply(Doc.of("ok", 0, "code", 11000, "errmsg", "duplicate key"));

        Throwable t = catchThrowable(() -> con.readSingleAnswer(REQUEST_ID));

        assertThat(t).isInstanceOf(MorphiumDriverException.class)
                .isNotInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(((MorphiumDriverException) t).getMongoCode()).isEqualTo(11000);
        assertThat(t).hasMessageContaining("duplicate key");
    }

    @Test
    public void doubleZeroOkIsAnErrorToo() throws Exception {
        SingleMongoConnection con = connect();
        writeReply(Doc.of("ok", 0.0, "code", 96, "errmsg", "operation failed"));

        Throwable t = catchThrowable(() -> con.readSingleAnswer(REQUEST_ID));

        assertThat(t).isInstanceOf(MorphiumDriverException.class);
        assertThat(((MorphiumDriverException) t).getMongoCode()).isEqualTo(96);
    }

    @Test
    public void integerOneOkIsSuccess() throws Exception {
        SingleMongoConnection con = connect();
        writeReply(Doc.of("ok", 1, "n", 1));

        Map<String, Object> answer = con.readSingleAnswer(REQUEST_ID);

        assertThat(answer.get("n")).isEqualTo(1);
        assertThat(con.isConnected()).as("a valid reply must not close the connection").isTrue();
    }

    @Test
    public void replyWithoutOkIsHandedUpRatherThanThrown() throws Exception {
        SingleMongoConnection con = connect();
        // not a shape any server sends, but it must not turn a reply's payload into an exception
        writeReply(Doc.of("n", 5));

        Map<String, Object> answer = con.readSingleAnswer(REQUEST_ID);

        assertThat(answer.get("n")).isEqualTo(5);
        assertThat(con.isConnected()).isTrue();
    }

    @Test
    public void replyWithNonNumericOkIsHandedUpRatherThanThrown() throws Exception {
        SingleMongoConnection con = connect();
        writeReply(Doc.of("ok", "1", "n", 7));

        Map<String, Object> answer = con.readSingleAnswer(REQUEST_ID);

        assertThat(answer.get("n")).isEqualTo(7);
    }

    @Test
    public void integerOneOkIsAcceptedOnTheReadPath() throws Exception {
        SingleMongoConnection con = connect();
        // checkForError() used to demand ok.equals(1.0): an int32 ok:1 made every find on such a
        // backend fail with "Error: 1 - null" although the cursor was right there
        writeReply(cursorReply(1));

        var cursor = con.getAnswerFor(REQUEST_ID, 10);

        assertNotNull(cursor);
        assertThat(cursor.getBatch()).hasSize(1);
        assertThat(cursor.getBatch().get(0)).containsEntry("x", 1);
    }

    @Test
    public void integerZeroOkIsRejectedOnTheReadPath() throws Exception {
        SingleMongoConnection con = connect();
        writeReply(Doc.of("ok", 0, "code", 96, "errmsg", "cursor not found"));

        Throwable t = catchThrowable(() -> con.getAnswerFor(REQUEST_ID, 10));

        assertThat(t).isInstanceOf(MorphiumDriverException.class);
        assertThat(((MorphiumDriverException) t).getMongoCode()).isEqualTo(96);
        assertThat(t).hasMessageContaining("cursor not found");
    }
}
