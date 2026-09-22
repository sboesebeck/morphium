package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * The manager configures 3 retries and 500 ms between them for the connection to its primary
 * - and got the driver's own 5 and 100 ms: {@code connectionSettings().setRetriesOnNetworkError}
 * is read by the ChangeStreamMonitor and the SequenceGenerator, never copied onto the driver,
 * so the stepdown retries of #393 ran five times per read against a source that had already
 * said it is not the primary ("retry 5/5" in the #364 logs). The budget is set on the driver
 * itself now.
 */
@Tag("server")
public class ReplicationManagerRetryBudgetTest {

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
        if (rm != null) {
            try {
                rm.stop();
            } catch (Exception ignored) {
            }
        }
        if (local != null) {
            try {
                local.close();
            } catch (Exception ignored) {
            }
        }
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    @Test
    public void theManagersRetryBudgetIsTheOneOnItsDriver() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        PoppyDB primary = new PoppyDB(port, "localhost", 20, 5);
        nodes.add(primary);
        primary.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port);
        rm.start();
        deadline = System.currentTimeMillis() + 15_000;
        while (rm.primaryDriverForTest() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(rm.primaryDriverForTest() != null, "the manager must have connected to its primary");

        assertEquals(3, rm.primaryDriverForTest().getRetriesOnNetworkError(), "retries the manager configured");
        assertEquals(500, rm.primaryDriverForTest().getSleepBetweenErrorRetries(), "sleep the manager configured");
    }
}
