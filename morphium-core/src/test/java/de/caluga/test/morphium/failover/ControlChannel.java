package de.caluga.test.morphium.failover;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

/**
 * Talks directly to one backend node port (never through a {@link de.caluga.test.morphium.testutil.proxy.WireProxy})
 * to discover replica-set membership and trigger elections - the control channel in the design
 * spec's two-channel architecture. Authenticates via SCRAM when {@code user} is non-null,
 * mirroring {@code UserFailoverTest#scramLoginWorks} but for issuing arbitrary commands rather
 * than just checking login success.
 */
public class ControlChannel implements AutoCloseable {
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final PooledDriver carrier = new PooledDriver();
    private final SingleMongoConnection con = new SingleMongoConnection();

    public ControlChannel(String host, int port, String authDb, String user, String password) throws MorphiumDriverException {
        carrier.setConnectionTimeout(5000);
        if (user != null) {
            con.setCredentials(authDb, user, password);
        }
        con.connect(carrier, host, port);
    }

    public Map<String, Object> command(Map<String, Object> cmd) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(MSG_ID.incrementAndGet());
        msg.setFirstDoc(cmd);
        OpMsg reply = con.sendAndWaitForReply(msg);
        // A clean EOF (peer closes at a message boundary, e.g. some replSetStepDown paths) makes
        // SingleMongoConnection.sendAndWaitForReply return null rather than throw - it is not an
        // I/O error, just "no reply came". Without this check, callers (including
        // commandTolerateClose) would NPE on reply.getFirstDoc() instead of seeing a clean,
        // catchable failure.
        if (reply == null) {
            throw new MorphiumDriverException("connection closed without a reply");
        }
        return reply.getFirstDoc();
    }

    /** Like {@link #command}, but tolerates the connection closing instead of replying - real
     * mongod does this for some {@code replSetStepDown} paths (see design spec's "Scenario
     * mapping"). Returns null in that case; callers must poll for the outcome instead. */
    public Map<String, Object> commandTolerateClose(Map<String, Object> cmd) {
        try {
            return command(cmd);
        } catch (MorphiumDriverException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> members() throws MorphiumDriverException {
        Map<String, Object> status = command(de.caluga.morphium.driver.Doc.of("replSetGetStatus", 1, "$db", "admin"));
        return (List<Map<String, Object>>) status.get("members");
    }

    public boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) return true;
            Thread.sleep(200);
        }
        return false;
    }

    @Override
    public void close() {
        try { con.close(); } catch (Exception ignored) { }
        try { carrier.close(); } catch (Exception ignored) { }
    }
}
