package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A change stream consumer that never received an event has no resume token: when its watch
 * dies, the restart begins at "now" and every event written during the gap is lost forever
 * (2026-07-20: m2 missed the question in AnsweringBasicTests because its msg_test stream was
 * mid-retry when the message was inserted).
 *
 * MongoDB's mechanism for exactly this case is the cursor's postBatchResumeToken, returned
 * with EVERY reply - including empty batches. watch() must capture it and publish the latest
 * token on the command (resumeAfter) on every exit path, so the caller can resume without a gap.
 */
@Tag("core")
public class WatchResumeTokenTest {

    private ServerSocket serverSocket;
    private Socket serverSide;
    private Socket clientSide;
    private Thread serverThread;

    @AfterEach
    public void tearDown() {
        for (AutoCloseable c : new AutoCloseable[] {serverSide, clientSide, serverSocket}) {
            if (c != null) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

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

    @Test
    public void publishesPostBatchResumeTokenOnConnectionDeath() throws Exception {
        SingleMongoConnection con = connect();
        CountDownLatch serverDone = new CountDownLatch(1);
        AtomicReference<Exception> serverError = new AtomicReference<>();

        serverThread = new Thread(() -> {
            try {
                InputStream sin = serverSide.getInputStream();
                OutputStream sout = serverSide.getOutputStream();

                // 1: the aggregate ($changeStream) request
                WireProtocolMessage aggregate = WireProtocolMessage.parseFromStream(sin);

                // 2: reply with an EMPTY batch that carries a postBatchResumeToken
                OpMsg reply = new OpMsg();
                reply.setMessageId(1);
                reply.setResponseTo(aggregate.getMessageId());
                reply.setFirstDoc(Doc.of(
                        "cursor", Doc.of(
                                "id", 12345L,
                                "ns", "testdb.testcoll",
                                "firstBatch", List.of(),
                                "postBatchResumeToken", Doc.of("_data", "TOKEN42")),
                        "ok", 1.0));
                sout.write(reply.bytes());
                sout.flush();

                // 3: the getMore request
                WireProtocolMessage.parseFromStream(sin);

                // 4: kill the stream mid-conversation with a garbage header (unknown opcode)
                sout.write(new byte[] {99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, (byte) 0xEF, (byte) 0xBE, 0, 0});
                sout.flush();
            } catch (Exception e) {
                serverError.set(e);
            } finally {
                serverDone.countDown();
            }
        });
        serverThread.start();

        WatchCommand cmd = new WatchCommand(con)
                .setDb("testdb")
                .setColl("testcoll")
                .setMaxTimeMS(200)
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                    }
                    @Override
                    public boolean isContinued() {
                        return true;
                    }
                });

        Throwable t = catchThrowable(cmd::watch);

        assertThat(serverDone.await(5, TimeUnit.SECONDS)).as("fake server finished").isTrue();
        assertThat(serverError.get()).as("fake server ran clean").isNull();
        assertThat(t).as("watch dies on the corrupted stream")
                .isInstanceOf(MorphiumDriverException.class);
        assertThat(cmd.getResumeAfter())
                .as("the postBatchResumeToken from the empty batch must survive the death of the "
                        + "watch, so the caller can resume without losing the events of the gap")
                .isNotNull()
                .containsEntry("_data", "TOKEN42");

        // Every reply - events or empty batch - is a liveness heartbeat: the server answers a
        // getMore within maxTimeMS. Consumers (ChangeStreamMonitor.isStreamLive) use this stamp
        // to tell a provably healthy stream from a silently dead one.
        assertThat(cmd.getLastReplyAt())
                .as("watch must stamp the time of the last server reply on the command")
                .isGreaterThan(0L);
    }
}
