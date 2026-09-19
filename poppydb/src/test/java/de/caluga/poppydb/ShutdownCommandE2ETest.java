package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #356 end to end: {@code {shutdown: 1}} over a real socket stops a real PoppyDB node - the
 * process no longer needs SIGTERM and shell access on the host for a rolling restart. Standalone
 * node on a free port, no election machinery, so this stays fast and always-on.
 */
@Tag("server")
public class ShutdownCommandE2ETest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);
    private PoppyDB srv;

    @AfterEach
    public void tearDown() {
        if (srv != null) {
            srv.shutdown(); // no-op when the command already did it
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static Map<String, Object> command(Socket sock, Map<String, Object> cmd) throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(MSG_ID.incrementAndGet());
        msg.setFlags(0);
        msg.setFirstDoc(cmd);
        sock.getOutputStream().write(msg.bytes());
        sock.getOutputStream().flush();
        OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
        return reply.getFirstDoc();
    }

    @Test
    public void shutdownOverTheWireStopsTheNode() throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "127.0.0.1", 20, 5);
        srv.start();
        assertThat(srv.isRunning()).isTrue();

        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            sock.setSoTimeout(15000);

            // the buffer is live: the startup line is in it before anything else happens
            Map<String, Object> log = command(sock, Doc.of("getLog", "global", "$db", "admin"));
            assertThat(log.get("ok")).as("reply: " + log).isEqualTo(1.0);
            @SuppressWarnings("unchecked")
            List<String> lines = (List<String>) log.get("log");
            assertThat(lines).anySatisfy(l -> assertThat(l).contains("PoppyDB started on 127.0.0.1:" + port));

            Map<String, Object> warnings = command(sock, Doc.of("getLog", "startupWarnings", "$db", "admin"));
            @SuppressWarnings("unchecked")
            List<String> warningLines = (List<String>) warnings.get("log");
            assertThat(warningLines)
                    .as("this node runs without --auth and without --dump-dir - both are worth a warning")
                    .anySatisfy(l -> assertThat(l).contains("Access control"))
                    .anySatisfy(l -> assertThat(l).contains("dump directory"));

            Map<String, Object> reply = command(sock, Doc.of("shutdown", 1, "$db", "admin"));
            assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        }

        long deadline = System.currentTimeMillis() + 20_000;

        while (srv.isRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertThat(srv.isRunning()).as("the node kept running after shutdown:1").isFalse();

        // and the port is really closed - not just a flag flipped
        long portDeadline = System.currentTimeMillis() + 10_000;
        boolean closed = false;

        while (!closed && System.currentTimeMillis() < portDeadline) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", port), 500);
                Thread.sleep(50);
            } catch (ConnectException e) {
                closed = true;
            }
        }

        assertThat(closed).as("port " + port + " still accepts connections after shutdown").isTrue();
    }
}
