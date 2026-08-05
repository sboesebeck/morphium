package de.caluga.test.morphium.failover;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.test.morphium.testutil.proxy.AddressRewriter;
import de.caluga.test.morphium.testutil.proxy.WireProxy;

/**
 * Backend-agnostic PooledDriver failover test: reproduces the 6.2.6 regressions (frozen
 * primary hangs in-flight operations until maxWaitTime instead of failing over) against
 * whichever replica set the test runner provides (MongoDB or PoppyDB), via a wire-rewriting
 * proxy instead of killing any process. Replaces the deleted, unrunnable
 * {@code FailoverReproTest} (see Task 6 for the migration). See
 * docs/superpowers/specs/2026-08-05-failover-proxy-test-design.md for the full design.
 */
@Tag("wire-failover")
public class DriverFailoverProxyTest {

    private static final Logger log = LoggerFactory.getLogger(DriverFailoverProxyTest.class);

    /** Plain entity, default write concern - deliberately NOT UncachedObject (see Global
     * Constraints: its WAIT_FOR_ALL_SLAVES write concern would distort failover timing). */
    @de.caluga.morphium.annotations.Entity
    public static class FoDoc {
        @de.caluga.morphium.annotations.Id
        public de.caluga.morphium.driver.MorphiumId id;
        public String strValue;
        public int counter;

        public FoDoc() { }
        public FoDoc(String s, int c) { strValue = s; counter = c; }
    }

    private final List<WireProxy> proxies = new ArrayList<>();
    private Morphium morphium;
    /** Tracked so {@link #tearDown()} can defensively interrupt/join it as a fallback - the
     * normal join happens in the scenario's own finally block, but if that is somehow skipped
     * (e.g. an error thrown outside the try) this stops a live thread from leaking past the
     * test (Global Constraints: zero live threads/sockets left behind). */
    private volatile Thread writerThread;

    @AfterEach
    void tearDown() {
        if (morphium != null) {
            try { morphium.close(); } catch (Exception ignored) { }
            morphium = null;
        }
        for (WireProxy p : proxies) {
            try { p.close(); } catch (Exception ignored) { }
        }
        proxies.clear();
        if (writerThread != null) {
            // Closing morphium/proxies above already unblocks a thread stuck inside store() on
            // a frozen connection; interrupt+join here is just a fallback for the sleep-between-
            // iterations case.
            writerThread.interrupt();
            try { writerThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            writerThread = null;
        }
    }

    // ---- backend discovery & proxy wiring (design spec: "Backend discovery & proxy wiring") ----

    private record Backend(String uri, String host, int port, String authDb, String user, String password) { }

    private Backend readBackend() {
        String uri = System.getProperty("morphium.uri");
        if (uri == null) uri = System.getenv("MONGODB_URI");
        if (uri == null) uri = System.getenv("MORPHIUM_URI");
        assumeTrue(uri != null && !uri.isBlank(), "no external backend configured - this test needs a real RS");

        // Minimal manual parse (avoids pulling in a full URI parser dependency): mongodb://[user:pass@]host1,host2,.../db
        String rest = uri.replaceFirst("^mongodb://", "");
        String userInfo = null;
        if (rest.contains("@")) {
            userInfo = rest.substring(0, rest.indexOf('@'));
            rest = rest.substring(rest.indexOf('@') + 1);
        }
        String hostsPart = rest.contains("/") ? rest.substring(0, rest.indexOf('/')) : rest;
        String firstHost = hostsPart.split(",")[0];
        String host = firstHost.contains(":") ? firstHost.substring(0, firstHost.indexOf(':')) : firstHost;
        int port = firstHost.contains(":") ? Integer.parseInt(firstHost.substring(firstHost.indexOf(':') + 1)) : 27017;
        assumeTrue(hostsPart.contains(","), "backend is not a replica set (single host) - this test needs an RS");

        String user = null, password = null;
        if (userInfo != null && userInfo.contains(":")) {
            user = userInfo.substring(0, userInfo.indexOf(':'));
            password = userInfo.substring(userInfo.indexOf(':') + 1);
        }
        return new Backend(uri, host, port, "admin", user, password);
    }

    private Map<String, String> wireProxies(Backend backend, List<Map<String, Object>> members) throws Exception {
        Map<String, String> backendToProxy = new HashMap<>();
        Map<String, WireProxy> proxyByBackend = new HashMap<>();
        for (Map<String, Object> m : members) {
            String name = (String) m.get("name");
            String mHost = name.contains(":") ? name.substring(0, name.indexOf(':')) : name;
            int mPort = Integer.parseInt(name.substring(name.indexOf(':') + 1));
            assumeTrue(isReachable(mHost, mPort), "member " + name + " is not reachable from this JVM");

            WireProxy proxy = new WireProxy(mHost, mPort);
            proxies.add(proxy);
            proxyByBackend.put(name, proxy);
            backendToProxy.put(name, "localhost:" + proxy.getListenPort());
        }
        AddressRewriter rewriter = new AddressRewriter(backendToProxy);
        for (WireProxy proxy : proxyByBackend.values()) {
            proxy.setRewriter(rewriter);
            proxy.start();
        }
        return backendToProxy;
    }

    private boolean isReachable(String host, int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), 2000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Morphium buildDriverUnderTest(Map<String, String> backendToProxy) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("wire_failover_test");
        cfg.clusterSettings().getHostSeed().clear();
        for (String proxyAddr : backendToProxy.values()) {
            cfg.clusterSettings().addHostToSeed(proxyAddr);
        }
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setRetriesOnNetworkError(10);
        cfg.connectionSettings().setSleepBetweenNetworkErrorRetries(1000);
        cfg.driverSettings().setRetryReads(true).setRetryWrites(true);
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);
        cfg.connectionSettings().setConnectionTimeout(5000);
        cfg.driverSettings().setReadTimeout(10000);
        cfg.clusterSettings().setHeartbeatFrequency(1000);
        cfg.connectionSettings().setMaxWaitTime(60000);
        // SSL and wire compression explicitly off - the proxy cannot frame-parse either
        // (see Global Constraints / design spec Non-Goals).
        cfg.connectionSettings().setUseSSL(false);
        cfg.driverSettings().setCompressionType(MorphiumConfig.CompressionType.NONE);
        return new Morphium(cfg);
    }

    /** Escape guard (design spec): the driver must only ever be connected to proxy addresses,
     * never a real backend address - otherwise a rewrite gap would let the test pass for the
     * wrong reason. */
    private void assertOnlyConnectedThroughProxies(Map<String, String> backendToProxy) {
        PooledDriver drv = (PooledDriver) morphium.getDriver();
        var proxyAddresses = new java.util.HashSet<>(backendToProxy.values());
        for (String connectedHost : drv.getNumConnectionsByHost().keySet()) {
            assertTrue(proxyAddresses.contains(connectedHost),
                    "driver connected to a non-proxy address " + connectedHost
                            + " - the address rewrite has a gap. Connected: " + drv.getNumConnectionsByHost()
                            + ", expected only: " + proxyAddresses);
        }
    }

    // ---- election helper (design spec: "Scenario mapping") ----

    /** Steps down the given (already-known) primary directly - no re-discovery, so this can
     * never race with the caller's own view of who the primary is (and which proxy got frozen). */
    private void stepDownPrimary(Backend backend, String primaryName) throws Exception {
        String[] hp = primaryName.split(":");
        try (ControlChannel primary = new ControlChannel(hp[0], Integer.parseInt(hp[1]),
                backend.authDb(), backend.user(), backend.password())) {
            // Real mongod may close the connection instead of replying for some stepdown paths -
            // both outcomes are acceptable (see design spec's "Scenario mapping").
            primary.commandTolerateClose(Doc.of("replSetStepDown", 60, "$db", "admin"));
        }
    }

    private boolean pollForNewPrimary(Backend backend, String exPrimaryName, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ControlChannel probe = new ControlChannel(backend.host(), backend.port(),
                    backend.authDb(), backend.user(), backend.password())) {
                boolean elected = probe.members().stream().anyMatch(m ->
                        "PRIMARY".equals(m.get("stateStr")) && !exPrimaryName.equals(m.get("name")));
                if (elected) return true;
            } catch (Exception ignored) {
                // control-channel node itself might be mid-election too - keep polling
            }
            Thread.sleep(200);
        }
        return false;
    }

    // ---- scenarios ----

    @Test
    void writesRecoverAfterFreeze() throws Exception {
        Backend backend = readBackend();
        List<Map<String, Object>> members;
        try (ControlChannel discover = new ControlChannel(backend.host(), backend.port(),
                backend.authDb(), backend.user(), backend.password())) {
            members = discover.members();
        }
        Map<String, String> backendToProxy = wireProxies(backend, members);
        morphium = buildDriverUnderTest(backendToProxy);
        morphium.dropCollection(FoDoc.class);
        Thread.sleep(500);
        assertOnlyConnectedThroughProxies(backendToProxy);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger writeOk = new AtomicInteger();
        Thread writer = new Thread(() -> {
            int i = 0;
            while (running.get()) {
                try {
                    morphium.store(new FoDoc("value" + i, i));
                    writeOk.incrementAndGet();
                } catch (Throwable t) {
                    log.debug("write failed (expected during the fault window): {}", t.getMessage());
                }
                i++;
                try { Thread.sleep(100); } catch (InterruptedException e) { return; }
            }
        }, "freeze-writer");
        writerThread = writer;
        writer.start();
        try {
            Thread.sleep(1000);
            assertTrue(writeOk.get() > 0, "no writes succeeded before the fault - harness itself is broken");

            // Find and freeze the current primary's proxy, then step it down.
            String primaryName;
            try (ControlChannel probe = new ControlChannel(backend.host(), backend.port(),
                    backend.authDb(), backend.user(), backend.password())) {
                primaryName = probe.members().stream()
                        .filter(m -> "PRIMARY".equals(m.get("stateStr")))
                        .map(m -> (String) m.get("name")).findFirst().orElseThrow();
            }
            String proxyAddr = backendToProxy.get(primaryName);
            WireProxy exPrimaryProxy = proxies.stream()
                    .filter(p -> ("localhost:" + p.getListenPort()).equals(proxyAddr))
                    .findFirst().orElseThrow();
            exPrimaryProxy.setFaultMode(de.caluga.test.morphium.testutil.proxy.FaultMode.freeze);
            stepDownPrimary(backend, primaryName);

            assertTrue(pollForNewPrimary(backend, primaryName, 15_000), "no new primary elected within 15s");

            // Timeout budget (Global Constraints): assert recovery within a few seconds of the
            // fault, well under maxWaitTime (60s) - a hang-until-maxWaitTime must NOT read as
            // "eventually recovered".
            int beforeRecoveryCheck = writeOk.get();
            long deadline = System.currentTimeMillis() + 10_000;
            boolean recovered = false;
            while (System.currentTimeMillis() < deadline) {
                if (writeOk.get() > beforeRecoveryCheck + 2) { recovered = true; break; }
                Thread.sleep(200);
            }
            assertTrue(recovered, "writes did not resume within 10s of the primary freezing + stepdown - "
                    + "driver is stuck on the frozen connection instead of failing over (writeOk stayed at "
                    + beforeRecoveryCheck + ")");
        } finally {
            // Must join here, not after the try - an assertTrue failure above (e.g. the 6.2.6
            // regression being reproduced: recovery not detected in time) must still not leak
            // this thread past the test, since tearDown() closes `morphium` right after and the
            // writer may still be blocked inside store() on that same instance.
            running.set(false);
            try {
                writer.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void writeReadRecoverAfterCleanStepdown() throws Exception {
        runWriteReadScenario(de.caluga.test.morphium.testutil.proxy.FaultMode.close);
    }

    @Test
    void writeReadRecoverAfterHardKill() throws Exception {
        runWriteReadScenario(de.caluga.test.morphium.testutil.proxy.FaultMode.reset);
    }

    private void runWriteReadScenario(de.caluga.test.morphium.testutil.proxy.FaultMode faultMode) throws Exception {
        Backend backend = readBackend();
        List<Map<String, Object>> members;
        try (ControlChannel discover = new ControlChannel(backend.host(), backend.port(),
                backend.authDb(), backend.user(), backend.password())) {
            members = discover.members();
        }
        Map<String, String> backendToProxy = wireProxies(backend, members);
        morphium = buildDriverUnderTest(backendToProxy);
        morphium.dropCollection(FoDoc.class);
        Thread.sleep(500);
        assertOnlyConnectedThroughProxies(backendToProxy);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger writeOk = new AtomicInteger();
        AtomicInteger readOk = new AtomicInteger();
        Thread writer = new Thread(() -> {
            int i = 0;
            while (running.get()) {
                try {
                    morphium.store(new FoDoc("value" + i, i));
                    writeOk.incrementAndGet();
                } catch (Throwable t) {
                    log.debug("write failed (expected during the fault window): {}", t.getMessage());
                }
                i++;
                try { Thread.sleep(200); } catch (InterruptedException e) { return; }
            }
        }, "writeread-writer");
        Thread reader = new Thread(() -> {
            while (running.get()) {
                try {
                    morphium.createQueryFor(FoDoc.class).countAll();
                    readOk.incrementAndGet();
                } catch (Throwable t) {
                    log.debug("read failed (expected during the fault window): {}", t.getMessage());
                }
                try { Thread.sleep(200); } catch (InterruptedException e) { return; }
            }
        }, "writeread-reader");
        writer.start();
        reader.start();
        try {
            Thread.sleep(1000);
            assertTrue(writeOk.get() > 0, "no writes succeeded before the fault - harness itself is broken");

            String primaryName;
            try (ControlChannel probe = new ControlChannel(backend.host(), backend.port(),
                    backend.authDb(), backend.user(), backend.password())) {
                primaryName = probe.members().stream()
                        .filter(m -> "PRIMARY".equals(m.get("stateStr")))
                        .map(m -> (String) m.get("name")).findFirst().orElseThrow();
            }
            String proxyAddr = backendToProxy.get(primaryName);
            WireProxy exPrimaryProxy = proxies.stream()
                    .filter(p -> ("localhost:" + p.getListenPort()).equals(proxyAddr))
                    .findFirst().orElseThrow();
            exPrimaryProxy.setFaultMode(faultMode);
            stepDownPrimary(backend, primaryName);

            assertTrue(pollForNewPrimary(backend, primaryName, 15_000), "no new primary elected within 15s");

            int writeBaseline = writeOk.get();
            int readBaseline = readOk.get();
            long deadline = System.currentTimeMillis() + 10_000;
            boolean recovered = false;
            while (System.currentTimeMillis() < deadline) {
                if (writeOk.get() > writeBaseline + 2 && readOk.get() > readBaseline + 2) { recovered = true; break; }
                Thread.sleep(200);
            }
            assertTrue(recovered, "writes/reads did not resume within 10s of the fault (" + faultMode + "): "
                    + "writeOk " + writeBaseline + " -> " + writeOk.get() + ", readOk " + readBaseline + " -> " + readOk.get());
        } finally {
            running.set(false);
        }
        writer.join(5000);
        reader.join(5000);
    }

    @Test
    void messagingRecoversAfterFailover() throws Exception {
        Backend backend = readBackend();
        List<Map<String, Object>> members;
        try (ControlChannel discover = new ControlChannel(backend.host(), backend.port(),
                backend.authDb(), backend.user(), backend.password())) {
            members = discover.members();
        }
        Map<String, String> backendToProxy = wireProxies(backend, members);
        morphium = buildDriverUnderTest(backendToProxy);
        Morphium receiverMorphium = buildDriverUnderTest(backendToProxy);
        try {
            AtomicInteger received = new AtomicInteger();
            var sender = morphium.createMessaging();
            sender.setSenderId("proxy-failover-sender");
            var receiver = receiverMorphium.createMessaging();
            receiver.setSenderId("proxy-failover-receiver");
            receiver.addListenerForTopic("wire_failover_test",
                    (de.caluga.morphium.messaging.MessageListener<de.caluga.morphium.messaging.Msg>) (msg, m) -> {
                        received.incrementAndGet();
                        return null;
                    });
            sender.start();
            receiver.start();
            Thread.sleep(1000);
            assertOnlyConnectedThroughProxies(backendToProxy);

            AtomicBoolean running = new AtomicBoolean(true);
            Thread sendThread = new Thread(() -> {
                int i = 0;
                while (running.get()) {
                    try {
                        sender.sendMessage(new de.caluga.morphium.messaging.Msg("wire_failover_test", "msg" + i, "value" + i));
                    } catch (Throwable t) {
                        log.debug("send failed (expected during the fault window): {}", t.getMessage());
                    }
                    i++;
                    try { Thread.sleep(300); } catch (InterruptedException e) { return; }
                }
            }, "messaging-sender");
            sendThread.start();
            try {
                Thread.sleep(1500);
                int receivedBefore = received.get();
                assertTrue(receivedBefore > 0, "no messages delivered before the fault - harness itself is broken");

                String primaryName;
                try (ControlChannel probe = new ControlChannel(backend.host(), backend.port(),
                        backend.authDb(), backend.user(), backend.password())) {
                    primaryName = probe.members().stream()
                            .filter(m -> "PRIMARY".equals(m.get("stateStr")))
                            .map(m -> (String) m.get("name")).findFirst().orElseThrow();
                }
                String proxyAddr = backendToProxy.get(primaryName);
                WireProxy exPrimaryProxy = proxies.stream()
                        .filter(p -> ("localhost:" + p.getListenPort()).equals(proxyAddr))
                        .findFirst().orElseThrow();
                exPrimaryProxy.setFaultMode(de.caluga.test.morphium.testutil.proxy.FaultMode.close);
                stepDownPrimary(backend, primaryName);

                assertTrue(pollForNewPrimary(backend, primaryName, 15_000), "no new primary elected within 15s");

                // Messaging goes through a changestream-resume path in addition to plain
                // read/write, so give it a little more room than the write/read scenarios (see
                // Post-plan follow-ups if this still proves flaky).
                int baseline = received.get();
                long deadline = System.currentTimeMillis() + 15_000;
                boolean recovered = false;
                while (System.currentTimeMillis() < deadline) {
                    if (received.get() > baseline + 1) { recovered = true; break; }
                    Thread.sleep(200);
                }
                assertTrue(recovered, "no messages delivered within 15s of the fault: received stayed at " + baseline);
            } finally {
                running.set(false);
                sendThread.join(5000);
                try { sender.terminate(); } catch (Exception ignored) { }
                try { receiver.terminate(); } catch (Exception ignored) { }
            }
        } finally {
            try { receiverMorphium.close(); } catch (Exception ignored) { }
        }
    }

    @Test
    void connectAfterElectionSucceedsWithoutTheOldPrimary() throws Exception {
        Backend backend = readBackend();
        List<Map<String, Object>> members;
        try (ControlChannel discover = new ControlChannel(backend.host(), backend.port(),
                backend.authDb(), backend.user(), backend.password())) {
            members = discover.members();
        }
        Map<String, String> backendToProxy = wireProxies(backend, members);

        // Fault + stepdown happen BEFORE Morphium exists - the application starts cold against an
        // already-elected new primary, with the old one unreachable (equivalent of the old test's
        // "primary dies HARD, replicaset elects a new primary, THEN the application starts").
        String primaryName;
        try (ControlChannel probe = new ControlChannel(backend.host(), backend.port(),
                backend.authDb(), backend.user(), backend.password())) {
            primaryName = probe.members().stream()
                    .filter(m -> "PRIMARY".equals(m.get("stateStr")))
                    .map(m -> (String) m.get("name")).findFirst().orElseThrow();
        }
        String proxyAddr = backendToProxy.get(primaryName);
        WireProxy exPrimaryProxy = proxies.stream()
                .filter(p -> ("localhost:" + p.getListenPort()).equals(proxyAddr))
                .findFirst().orElseThrow();
        exPrimaryProxy.setFaultMode(de.caluga.test.morphium.testutil.proxy.FaultMode.reset);
        stepDownPrimary(backend, primaryName);
        assertTrue(pollForNewPrimary(backend, primaryName, 15_000), "no new primary elected within 15s");

        morphium = buildDriverUnderTest(backendToProxy);
        assertOnlyConnectedThroughProxies(backendToProxy);
        FoDoc o = new FoDoc("afterRestart", 42);
        morphium.store(o);
        long cnt = morphium.createQueryFor(FoDoc.class).f("strValue").eq("afterRestart").countAll();
        assertTrue(cnt > 0, "write after cold-start-post-election not readable");
    }
}
