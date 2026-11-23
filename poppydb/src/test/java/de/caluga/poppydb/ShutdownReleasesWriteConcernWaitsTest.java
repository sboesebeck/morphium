package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * #529: {@code shutdown()} releases the write-concern waits of a primary while it can still
 * answer them. {@code stopElection()} does not go through the step-down path, so without an
 * explicit close the waits stayed in the shared timer until their deadline and then hit a closed
 * event loop; the client lost its connection instead of getting an answer.
 */
@Tag("server")
public class ShutdownReleasesWriteConcernWaitsTest {

    private PoppyDB primary;

    @AfterEach
    public void tearDown() {
        if (primary != null) {
            primary.shutdown();
        }
    }

    private static int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @Timeout(60)
    public void shutdownAnswersAPendingWaitAsPrimarySteppedDown() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        // Static two-member set; the second member is never started.
        primary = new PoppyDB(port1, "localhost", 10, 1);
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2);
        primary.configureReplicaSet("rsShutdownWaits", hosts, Map.of(hosts.get(0), 300, hosts.get(1), 100));
        primary.start();

        ReplicationCoordinator coordinator = null;
        long deadline = System.currentTimeMillis() + 10_000;
        while (coordinator == null && System.currentTimeMillis() < deadline) {
            coordinator = primary.getReplicationCoordinator();
            Thread.sleep(50);
        }
        assertNotNull(coordinator, "a static-mode primary has a replication coordinator");
        // A live secondary that never gets anywhere: the wait neither takes the "no secondaries"
        // fast path nor is it ever acknowledged.
        coordinator.reportProgress(hosts.get(1), -1);
        int timerTasksBefore = ReplicationCoordinator.pendingTimerTasksForTest();

        CompletableFuture<Map<String, Object>> reply = CompletableFuture.supplyAsync(() -> {
            try (Socket sock = new Socket()) {
                sock.connect(new InetSocketAddress("localhost", port1), 2000);
                sock.setSoTimeout(30_000);
                OpMsg msg = new OpMsg();
                msg.setMessageId(1);
                msg.setFirstDoc(Doc.of("insert", "docs", "$db", "shutdownwaits",
                        "documents", List.of(Doc.of("_id", "pending")),
                        "writeConcern", Doc.of("w", 2, "wtimeout", 60_000)));
                sock.getOutputStream().write(msg.bytes());
                sock.getOutputStream().flush();
                return ((OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream())).getFirstDoc();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final ReplicationCoordinator waitingOn = coordinator;
        deadline = System.currentTimeMillis() + 10_000;
        while (((Number) waitingOn.getWaitStats().get("waiting")).intValue() < 1
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, waitingOn.getWaitStats().get("waiting"), "the w=2 write must be waiting");

        primary.shutdown();

        Map<String, Object> answer = reply.get(10, TimeUnit.SECONDS);
        assertEquals(1.0, ((Number) answer.get("ok")).doubleValue(), "the write was executed: " + answer);
        Map<String, Object> wce = (Map<String, Object>) answer.get("writeConcernError");
        assertNotNull(wce, "the pending wait must be answered on shutdown: " + answer);
        assertEquals(189, ((Number) wce.get("code")).intValue());

        assertEquals(0, waitingOn.getWaitStats().get("waiting"));
        assertTrue(ReplicationCoordinator.pendingTimerTasksForTest() <= timerTasksBefore,
                "the released wait must not leave its deadline task in the shared timer");
    }
}
