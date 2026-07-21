package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;

import de.caluga.morphium.driver.MorphiumDriverNetworkException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * readNextMessage used to treat every socket timeout as benign: it retried the parse on the
 * same stream and, once the deadline was reached, returned null while leaving the connection
 * open. Both are wrong when the timeout struck mid-message (header consumed, body pending):
 * the retry reads payload bytes as a header ("Illegal opcode"), and the open-after-null
 * connection is a landmine for the next pool borrower (2026-07-20 overnight run,
 * AnsweringBasicTests on mongodb_single).
 */
@Tag("core")
public class SingleMongoConnectionTimeoutTest {

    private ServerSocket serverSocket;
    private Socket serverSide;
    private Socket clientSide;

    private SingleMongoConnection connect() throws Exception {
        serverSocket = new ServerSocket(0);
        clientSide = new Socket("127.0.0.1", serverSocket.getLocalPort());
        serverSide = serverSocket.accept();

        SingleMongoConnection con = new SingleMongoConnection();
        set(con, "s", clientSide);
        set(con, "in", clientSide.getInputStream());
        set(con, "out", clientSide.getOutputStream());
        set(con, "connected", true);
        set(con, "connectedTo", "localhost-test");
        set(con, "driver", new PooledDriver());
        return con;
    }

    private static void set(SingleMongoConnection con, String field, Object value) throws Exception {
        Field f = SingleMongoConnection.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(con, value);
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

    private static byte[] opMsgHeader(int totalSize) {
        byte[] h = new byte[16];
        writeInt(h, 0, totalSize);
        writeInt(h, 4, 4711);       // messageId
        writeInt(h, 8, 0);          // responseTo
        writeInt(h, 12, 2013);      // OP_MSG
        return h;
    }

    private static void writeInt(byte[] b, int off, int v) {
        b[off] = (byte) (v & 0xff);
        b[off + 1] = (byte) ((v >> 8) & 0xff);
        b[off + 2] = (byte) ((v >> 16) & 0xff);
        b[off + 3] = (byte) ((v >> 24) & 0xff);
    }

    @Test
    public void midMessageTimeoutClosesConnectionAndThrows() throws Exception {
        SingleMongoConnection con = connect();

        // server announces a 100-byte message but never sends the body
        OutputStream serverOut = serverSide.getOutputStream();
        serverOut.write(opMsgHeader(100));
        serverOut.flush();

        Throwable t = catchThrowable(() -> con.readNextMessage(500));

        assertThat(t).as("mid-message timeout desyncs the stream - must be fatal, not a null return")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(con.isConnected()).as("desynced connection must be closed").isFalse();
    }

    @Test
    public void deadlineTimeoutClosesConnection() throws Exception {
        SingleMongoConnection con = connect();
        // server stays completely silent - clean timeout at a message boundary

        var reply = con.readNextMessage(300);

        assertThat(reply).isNull();
        assertThat(con.isConnected())
                .as("a connection whose reply never came may still receive it later - "
                        + "handing it back to the pool would serve a stale reply to the next borrower")
                .isFalse();
    }
}
