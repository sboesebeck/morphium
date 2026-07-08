package de.caluga.test.morphium.failover;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

/**
 * Manual failover reproduction against the local 3-node replicaset from ~/mongo
 * (ports 27017-27019, rs "test", auth test/test on admin).
 *
 * Node 1 (27017) has priority 100 and is expected to be primary at test start.
 * The test kills it via ~/mongo/stopNode.sh 1 and restarts it via startNode.sh.
 */
// "manual" is excluded by default AND by the -Pexternal profile - these tests need
// the local replicaset from ~/mongo and kill/freeze mongod processes, so they must
// never run in CI (external phases run with -Pexternal, which enables "external"
// tagged tests, but not "manual" ones). Run manually via:
//   mvn -pl morphium-core test -Dtest=FailoverReproTest -Dtest.excludeTags=
@Tag("manual")
@Tag("external")
@Tag("failover")
@Tag("failoverRepro")
public class FailoverReproTest {

    /**
     * Plain entity with default write concern. Deliberately NOT UncachedObject:
     * that one is annotated WAIT_FOR_ALL_SLAVES(timeout 10s), which makes every
     * write block server-side for 10s while a replicaset member is down and
     * would completely distort failover timing measurements.
     */
    @de.caluga.morphium.annotations.Entity
    public static class FoDoc {
        @de.caluga.morphium.annotations.Id
        public de.caluga.morphium.driver.MorphiumId id;
        public String strValue;
        public int counter;

        public FoDoc() {
        }

        public FoDoc(String s, int c) {
            strValue = s;
            counter = c;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FailoverReproTest.class);
    private static final String MONGO_DIR = System.getProperty("user.home") + "/mongo";

    private Morphium morphium;

    private MorphiumConfig getCfg() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("failover_repro");
        cfg.clusterSettings().getHostSeed().clear();
        cfg.clusterSettings().addHostToSeed("localhost:27017");
        cfg.clusterSettings().addHostToSeed("localhost:27018");
        cfg.clusterSettings().addHostToSeed("localhost:27019");
        cfg.authSettings().setMongoLogin("test").setMongoPassword("test").setMongoAuthDb("admin");
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setRetriesOnNetworkError(10);
        cfg.connectionSettings().setSleepBetweenNetworkErrorRetries(1000);
        cfg.driverSettings().setRetryReads(true).setRetryWrites(true);
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);
        cfg.connectionSettings().setConnectionTimeout(5000);
        cfg.driverSettings().setReadTimeout(10000);
        cfg.clusterSettings().setHeartbeatFrequency(1000);
        return cfg;
    }

    private void killNode(int nr) throws Exception {
        log.info("### KILLING node {} ###", nr);
        new ProcessBuilder("/bin/bash", MONGO_DIR + "/stopNode.sh", String.valueOf(nr))
            .directory(new java.io.File(MONGO_DIR)).inheritIO().start().waitFor();
    }

    /** kill -9: no clean stepdown, no connection close - simulates real node/VM failure */
    private void killNodeHard(int nr) throws Exception {
        log.info("### HARD-KILLING node {} (kill -9) ###", nr);
        new ProcessBuilder("/bin/bash", "-c",
            "kill -9 $(cat " + MONGO_DIR + "/data/mongo" + nr + ".pid) && rm -f " + MONGO_DIR + "/data/mongo" + nr + ".pid")
            .inheritIO().start().waitFor();
    }

    /**
     * kill -STOP: freezes the process. TCP connections stay open but never answer -
     * closest local simulation of a dead VM / network partition (no RST packets).
     */
    private void freezeNode(int nr) throws Exception {
        log.info("### FREEZING node {} (kill -STOP) ###", nr);
        new ProcessBuilder("/bin/bash", "-c",
            "kill -STOP $(cat " + MONGO_DIR + "/data/mongo" + nr + ".pid)")
            .inheritIO().start().waitFor();
    }

    private void unfreezeNode(int nr) throws Exception {
        new ProcessBuilder("/bin/bash", "-c",
            "kill -CONT $(cat " + MONGO_DIR + "/data/mongo" + nr + ".pid) 2>/dev/null || true")
            .inheritIO().start().waitFor();
    }

    private void startNode(int nr) throws Exception {
        log.info("### RESTARTING node {} ###", nr);
        new ProcessBuilder("/bin/bash", MONGO_DIR + "/startNode.sh", "--nodel", String.valueOf(nr), "test")
            .directory(new java.io.File(MONGO_DIR)).inheritIO().start().waitFor();
    }

    @Test
    public void writesRecoverAfterPrimaryFreeze() throws Exception {
        // Simulates a dead VM / network partition: the primary's sockets stay open
        // but never answer (no RST). Uses production-like maxWaitTime, which is the
        // socket read timeout for in-flight operations.
        MorphiumConfig cfg = getCfg();
        cfg.connectionSettings().setMaxWaitTime(60000);
        morphium = new Morphium(cfg);
        morphium.dropCollection(FoDoc.class);
        Thread.sleep(1000);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger writeOk = new AtomicInteger();
        AtomicInteger writeErr = new AtomicInteger();
        Thread writer = new Thread(() -> {
            int i = 0;
            while (running.get()) {
                try {
                    morphium.store(new FoDoc("value" + i, i));
                    writeOk.incrementAndGet();
                } catch (Throwable t) {
                    writeErr.incrementAndGet();
                    log.error("WRITE FAILED: {}", t.getMessage());
                }
                i++;
                try { Thread.sleep(100); } catch (InterruptedException e) { return; }
            }
        }, "frozen-writer");
        writer.start();
        Thread.sleep(3000);
        assertTrue(writeOk.get() > 0, "no writes before freeze");

        freezeNode(1);
        try {
            // The writer has an operation in flight on a frozen connection. Election
            // happens after ~10s. The driver must detect the frozen host, close its
            // connections and let the (retried) write continue on the new primary -
            // NOT hang until maxWaitTime (60s).
            de.caluga.morphium.driver.wire.PooledDriver drv = (de.caluga.morphium.driver.wire.PooledDriver) morphium.getDriver();
            for (int i = 5; i <= 30; i += 5) {
                Thread.sleep(5000);
                log.info("{}s AFTER FREEZE: writeOk={} writeErr={} primaryNode={} connections={}",
                    i, writeOk.get(), writeErr.get(), drv.getPrimaryNode(), drv.getNumConnectionsByHost());
            }
            int okAt30 = writeOk.get();

            Thread.sleep(10000);
            int okAt40 = writeOk.get();
            log.info("40s AFTER FREEZE: writeOk={} writeErr={} primaryNode={} connections={}",
                okAt40, writeErr.get(), drv.getPrimaryNode(), drv.getNumConnectionsByHost());
            assertTrue(okAt40 > okAt30 + 5,
                "writer is still stuck 30-40s after primary froze (in-flight op hangs until maxWaitTime, frozen host not evicted): "
                + okAt30 + " -> " + okAt40);
        } finally {
            running.set(false);
            unfreezeNode(1);
        }
        writer.join(5000);
    }

    @AfterEach
    public void cleanup() throws Exception {
        // make sure node 1 is unfrozen and running again for the next test
        unfreezeNode(1);
        Thread.sleep(500);
        startNode(1);
        Thread.sleep(2000);
        if (morphium != null) {
            try {
                morphium.close();
            } catch (Exception e) {
                log.warn("close failed", e);
            }
            morphium = null;
        }
    }

    @Test
    public void writeReadDuringPrimaryHardFailover() throws Exception {
        runWriteReadScenario(true);
    }

    @Test
    public void writeReadDuringPrimaryFailover() throws Exception {
        runWriteReadScenario(false);
    }

    @Test
    public void messagingDuringPrimaryHardFailover() throws Exception {
        runMessagingScenario(true);
    }

    @Test
    public void restartAfterPrimaryFailure() throws Exception {
        // primary dies HARD, replicaset elects a new primary, THEN the application starts.
        killNodeHard(1);
        log.info("Waiting for election of new primary...");
        long start = System.currentTimeMillis();
        boolean elected = false;
        while (System.currentTimeMillis() - start < 60000) {
            try {
                Process p = new ProcessBuilder("/bin/bash", "-c",
                    System.getProperty("user.home") + "/mongo/mongosh-2.2.6-darwin-arm64/bin/mongosh \"mongodb://test:test@127.0.0.1:27018/admin\" --quiet --eval 'print(db.hello().isWritablePrimary || rs.status().members.some(m=>m.stateStr==\"PRIMARY\"))'").start();
                p.waitFor();
                String out = new String(p.getInputStream().readAllBytes()).trim();
                if (out.contains("true")) { elected = true; break; }
            } catch (Exception e) {
                log.warn("check failed: {}", e.getMessage());
            }
            Thread.sleep(2000);
        }
        assertTrue(elected, "replicaset did not elect a new primary within 60s");
        log.info("New primary elected. Now starting Morphium (node 1 still down)...");

        morphium = new Morphium(getCfg());
        FoDoc o = new FoDoc("afterRestart", 42);
        morphium.store(o);
        long cnt = morphium.createQueryFor(FoDoc.class).f("str_value").eq("afterRestart").countAll();
        log.info("RESTART TEST: connect + write + read OK, count={}", cnt);
        assertTrue(cnt > 0, "write after restart not readable");
    }

    private void runWriteReadScenario(boolean hardKill) throws Exception {
        morphium = new Morphium(getCfg());
        morphium.dropCollection(FoDoc.class);
        Thread.sleep(1000);

        AtomicInteger writeOk = new AtomicInteger();
        AtomicInteger writeErr = new AtomicInteger();
        AtomicInteger readOk = new AtomicInteger();
        AtomicInteger readErr = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);

        Thread writer = new Thread(() -> {
            int i = 0;
            while (running.get()) {
                try {
                    morphium.store(new FoDoc("value" + i, i));
                    writeOk.incrementAndGet();
                } catch (Throwable t) {
                    writeErr.incrementAndGet();
                    log.error("WRITE FAILED: {}", t.getMessage());
                }
                i++;
                try { Thread.sleep(200); } catch (InterruptedException e) { return; }
            }
        }, "writer");

        Thread reader = new Thread(() -> {
            while (running.get()) {
                try {
morphium.createQueryFor(FoDoc.class).countAll();
                    readOk.incrementAndGet();
                } catch (Throwable t) {
                    readErr.incrementAndGet();
                    log.error("READ FAILED: {}", t.getMessage());
                }
                try { Thread.sleep(200); } catch (InterruptedException e) { return; }
            }
        }, "reader");

        writer.start();
        reader.start();

        // let it run against healthy RS
        Thread.sleep(5000);
        log.info("BEFORE KILL: writeOk={} writeErr={} readOk={} readErr={}", writeOk.get(), writeErr.get(), readOk.get(), readErr.get());
        assertTrue(writeOk.get() > 0, "no writes succeeded even before failover");

        if (hardKill) killNodeHard(1); else killNode(1);

        // election timeout is 10s in this cluster; give it 45s to recover
        Thread.sleep(45000);
        int wOkAfterElection = writeOk.get();
        log.info("45s AFTER KILL: writeOk={} writeErr={} readOk={} readErr={}", writeOk.get(), writeErr.get(), readOk.get(), readErr.get());

        // now measure recovery: do writes succeed in the next 20s?
        Thread.sleep(20000);
        int wOkFinal = writeOk.get();
        log.info("FINAL: writeOk={} writeErr={} readOk={} readErr={}", writeOk.get(), writeErr.get(), readOk.get(), readErr.get());

        running.set(false);
        writer.join(5000);
        reader.join(5000);

        long stored = -1;
        try {
            stored = morphium.createQueryFor(FoDoc.class).countAll();
        } catch (Exception e) {
            log.error("final count failed: {}", e.getMessage());
        }
        log.info("Documents in collection at end: {}", stored);

        assertTrue(wOkFinal > wOkAfterElection,
            "NO WRITES SUCCEEDED after failover recovery window (writes stuck): " + wOkAfterElection + " -> " + wOkFinal);
    }

    @Test
    public void messagingDuringPrimaryFailover() throws Exception {
        runMessagingScenario(false);
    }

    private void runMessagingScenario(boolean hardKill) throws Exception {
        morphium = new Morphium(getCfg());
        MorphiumConfig cfg2 = getCfg();
        Morphium m2 = new Morphium(cfg2);

        AtomicInteger received = new AtomicInteger();
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setSenderId("sender");
        MorphiumMessaging receiver = m2.createMessaging();
        receiver.setSenderId("receiver");
        receiver.addListenerForTopic("failover_test", (MessageListener<Msg>) (msg, m) -> {
            received.incrementAndGet();
            return null;
        });
        sender.start();
        receiver.start();
        Thread.sleep(2000);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger sendOk = new AtomicInteger();
        AtomicInteger sendErr = new AtomicInteger();

        Thread sendThread = new Thread(() -> {
            int i = 0;
            while (running.get()) {
                try {
                    Msg msg = new Msg("failover_test", "msg" + i, "value" + i);
                    sender.sendMessage(msg);
                    sendOk.incrementAndGet();
                } catch (Throwable t) {
                    sendErr.incrementAndGet();
                    log.error("SEND FAILED: {}", t.getMessage());
                }
                i++;
                try { Thread.sleep(500); } catch (InterruptedException e) { return; }
            }
        }, "msg-sender");
        sendThread.start();

        Thread.sleep(5000);
        int receivedBefore = received.get();
        log.info("BEFORE KILL: sent={} sendErr={} received={}", sendOk.get(), sendErr.get(), receivedBefore);
        assertTrue(receivedBefore > 0, "messaging not working even before failover");

        if (hardKill) killNodeHard(1); else killNode(1);

        Thread.sleep(45000);
        int receivedAfterElection = received.get();
        log.info("45s AFTER KILL: sent={} sendErr={} received={}", sendOk.get(), sendErr.get(), receivedAfterElection);

        // measure recovery: are NEW messages delivered in the next 30s?
        Thread.sleep(30000);
        int receivedFinal = received.get();
        log.info("FINAL: sent={} sendErr={} received={}", sendOk.get(), sendErr.get(), receivedFinal);

        running.set(false);
        sendThread.join(5000);
        try {
            sender.terminate();
            receiver.terminate();
        } catch (Exception e) {
            log.warn("terminate failed: {}", e.getMessage());
        }
        m2.close();

        assertTrue(receivedFinal > receivedAfterElection,
            "NO MESSAGES DELIVERED after failover (changestream/messaging stuck): "
            + receivedAfterElection + " -> " + receivedFinal);
    }
}
