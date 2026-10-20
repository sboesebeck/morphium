package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverNoReplyException;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A request whose reply never arrives must surface as a <em>retryable network error</em>, not as
 * {@code null}. The read path used to return null for "peer closed at a message boundary" and for
 * "reply missed the deadline"; the command wrappers then either NPE'd on the answer
 * ({@code res.get(...)} in listDatabases / replSetGetStatus / distinct / currentOp) or - worse for
 * reads - turned it into a silently empty result set.
 *
 * <p>Production trigger: a connection that sat idle in the pool until the <em>server</em> dropped
 * it (its own connection-pool expiry, a restart, a load balancer's idle timeout). The pooled
 * connection still looks connected locally, so the next borrower's request goes out on a dead
 * socket and the reply never comes.
 */
@Tag("core")
public class SingleMongoConnectionNoReplyTest {

    private ServerSocket serverSocket;
    private Socket serverSide;
    private Socket clientSide;
    private SingleMongoConnection con;

    private SingleMongoConnection connect(int maxWaitTime) throws Exception {
        serverSocket = new ServerSocket(0);
        clientSide = new Socket("127.0.0.1", serverSocket.getLocalPort());
        serverSide = serverSocket.accept();

        PooledDriver driver = new PooledDriver();
        driver.setMaxWaitTime(maxWaitTime);

        con = new SingleMongoConnection();
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
    public void peerClosedConnectionYieldsRetryableNetworkError() throws Exception {
        SingleMongoConnection c = connect(500);
        // what a server does to an idle pooled connection: a clean FIN, no reply
        serverSide.close();

        Throwable t = catchThrowable(() -> c.readSingleAnswer(4711));

        assertThat(t).as("no reply must not be handed up as null")
                .isInstanceOf(MorphiumDriverNoReplyException.class);
        assertThat(t).as("callers retry network errors - a missing reply must be one")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(c.isConnected()).as("the connection can never serve another request").isFalse();
    }

    @Test
    public void replyMissedDeadlineYieldsRetryableNetworkError() throws Exception {
        SingleMongoConnection c = connect(300);
        // server stays completely silent: no EOF, just no answer
        long start = System.currentTimeMillis();

        Throwable t = catchThrowable(() -> c.readSingleAnswer(4711));

        assertThat(t).isInstanceOf(MorphiumDriverNoReplyException.class);
        assertThat(System.currentTimeMillis() - start)
                .as("must fail within the caller's budget, not block")
                .isLessThan(5_000);
        assertThat(c.isConnected()).isFalse();
    }

    @Test
    public void getAnswerForThrowsInsteadOfReturningAnEmptyCursor() throws Exception {
        SingleMongoConnection c = connect(500);
        serverSide.close();

        Throwable t = catchThrowable(() -> c.getAnswerFor(4711, 10));

        assertThat(t).as("an empty cursor here reads as 'no more documents' and silently truncates a result set")
                .isInstanceOf(MorphiumDriverNoReplyException.class);
    }

    @Test
    public void readAnswerForThrowsInsteadOfReturningAnEmptyList() throws Exception {
        SingleMongoConnection c = connect(500);
        serverSide.close();

        Throwable t = catchThrowable(() -> c.readAnswerFor(4711));

        assertThat(t).isInstanceOf(MorphiumDriverNoReplyException.class);
    }

    @Test
    public void anArrivingReplyIsStillDelivered() throws Exception {
        SingleMongoConnection c = connect(2000);

        OpMsg resp = new OpMsg();
        resp.setMessageId(4712);
        resp.setResponseTo(4711);
        resp.setFirstDoc(Doc.of("ok", 1.0, "cursor", Doc.of("id", 0L, "ns", "test.coll",
                                 "firstBatch", List.of(Doc.of("x", 1)))));
        serverSide.getOutputStream().write(resp.bytes());
        serverSide.getOutputStream().flush();

        var cursor = c.getAnswerFor(4711, 10);

        assertNotNull(cursor);
        assertThat(cursor.getBatch()).hasSize(1);
        assertThat(cursor.getBatch().get(0)).containsEntry("x", 1);
        assertThat(c.isConnected()).as("a served request must not close the connection").isTrue();
    }
}
