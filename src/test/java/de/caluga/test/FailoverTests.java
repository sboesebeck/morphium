package de.caluga.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.UncachedObject;

@Disabled
public class FailoverTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void primaryFailoverTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            morphium.getConfig().setRetriesOnNetworkError(10);
            morphium.getConfig().setSleepBetweenNetworkErrorRetries(1500);
            TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
            var originalConnectedTo = "";

            for (int i = 0; i < 1000; i++) {
                if (i == 500) {
                    originalConnectedTo = shutdownPrimary(morphium);
                }

                try {
                    morphium.store(new UncachedObject("Str" + i, i));
                } catch (Exception el) {
                    log.error("Got an error: " + el.getMessage(), el);
                }
            }

            long countAll = morphium.createQueryFor(UncachedObject.class).countAll();
            log.info("Wrote 1000 documents, now in db " + countAll);
            assertTrue(990 < countAll);
            Thread.sleep(1000);
            log.info("Please restart the node that was shut down!");
            checkCluster(morphium, originalConnectedTo);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void primaryFailoverMessagingTest(Morphium morphium) throws Exception {
        var l = (Logger)LoggerFactory.getLogger(PooledDriver.class);
        l.setLevel(Level.OFF);
        l = (Logger)LoggerFactory.getLogger(SingleMongoConnectDriver.class);
        l.setLevel(Level.OFF);

        try (morphium) {
            MorphiumMessaging sender = morphium.createMessaging();
            sender.start();
            MorphiumMessaging receiver = morphium.createMessaging();
            receiver.start();
            receiver.addListenerForTopic("Test", (m, msg)-> {
                return msg.createAnswerMsg();
            });
            Thread.sleep(3000);
            AtomicBoolean b = new AtomicBoolean(true);
            AtomicInteger unanswered = new AtomicInteger(0);
            AtomicInteger answers = new AtomicInteger(0);
            new Thread(()-> {
                while (b.get()) {
                    Msg m = new Msg("Test", "value", "Val");
                    var answer = sender.sendAndAwaitFirstAnswer(m, 500, false);

                    if (answer == null) {
                        log.error("Did not get answer...");
                        unanswered.incrementAndGet();
                    } else {
                        log.info("Answer incoming");
                        answers.incrementAndGet();
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }).start();
            Thread.sleep(5000);
            String originalConnectedTo = shutdownPrimary(morphium);
            Thread.sleep(5000);

            for (int i = 0; i < 10; i++) {
                log.info("Errors after shutdown: " + unanswered.get() + " - received: " + answers.get());
                Thread.sleep(1000);
            }

            b.set(false);
            Thread.sleep(2000);
            sender.terminate();
            receiver.terminate();
            checkCluster(morphium, originalConnectedTo);
        }
    }

    private String shutdownPrimary(Morphium morphium) throws MorphiumDriverException, InterruptedException {
        var drv = morphium.getDriver();
        var con = drv.getPrimaryConnection(morphium.getWriteConcernForClass(UncachedObject.class));
        String connectedTo = con.getConnectedTo();

        try {
            ShutdownCommand shutdown = new ShutdownCommand(con);
            shutdown.setDb("admin").setComment("Shutting down for testing").setTimeoutSecs(3);
            var ret = shutdown.executeAsync();
            log.info("Shutdown in progress - command " + ret);
            Thread.sleep(5000);
        } finally {
            drv.releaseConnection(con);
        }

        return connectedTo;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void primaryFailoverChangestream(Morphium morphium) throws Exception {
        morphium.getConfig().setMaxConnectionIdleTime(1000);

        try (morphium) {
            AtomicInteger incomingEvent = new AtomicInteger(0);
            ChangeStreamMonitor monitor = new ChangeStreamMonitor(morphium, UncachedObject.class);
            monitor.addListener((evt)-> {
                log.info("Incoming...");
                incomingEvent.incrementAndGet();
                return true;
            });
            monitor.start();
            Thread.sleep(2000);
            assertEquals(0, incomingEvent.get());
            morphium.store(new UncachedObject("test", 123));
            Thread.sleep(1100);
            assertEquals(1, incomingEvent.get());
            String originalConnectedTo = shutdownPrimary(morphium);
            Thread.sleep(2 * morphium.getConfig().getMaxConnectionIdleTime());
            morphium.store(new UncachedObject("test", 123));
            Thread.sleep(10000);
            assertEquals(2, incomingEvent.get());
            monitor.terminate();
            checkCluster(morphium, originalConnectedTo);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesSingleOnly")
    public void primaryFailoverTestSingle(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            morphium.getConfig().setRetriesOnNetworkError(10);
            morphium.getConfig().setSleepBetweenNetworkErrorRetries(1500);
            TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
            String originalConnectedTo = "";

            for (int i = 0; i < 1000; i++) {
                if (i == 500) {
                    originalConnectedTo = shutdownPrimary(morphium);
                }

                try {
                    morphium.store(new UncachedObject("Str" + i, i));
                } catch (Exception el) {
                    log.error("Got an error: " + el.getMessage(), el);
                }
            }

            long countAll = morphium.createQueryFor(UncachedObject.class).countAll();
            log.info("Wrote 1000 documents, now in db " + countAll);
            assertTrue(990 < countAll);
            Thread.sleep(1000);
            checkCluster(morphium, originalConnectedTo);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesSingleOnly")
    public void primaryFailoverWatchTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            morphium.getConfig().setRetriesOnNetworkError(10);
            morphium.getConfig().setSleepBetweenNetworkErrorRetries(1500);
            TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicInteger events = new AtomicInteger(0);
            new Thread(()-> {
                var enc = new AESEncryptionProvider();
                enc.setEncryptionKey("1234567890abcdef".getBytes());
                enc.setDecryptionKey("1234567890abcdef".getBytes());
                var password = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
                var user = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
                var authDb = Base64.getEncoder().encodeToString(enc.encrypt("admin".getBytes(StandardCharsets.UTF_8)));
                MorphiumConfig singleConnection = MorphiumConfig.fromProperties(getProps());
                singleConnection.setDriverName(SingleMongoConnectDriver.driverName);
                singleConnection.setCredentialsEncrypted(true);
                singleConnection.setCredentialsEncryptionKey("1234567890abcdef");
                singleConnection.setCredentialsDecryptionKey("1234567890abcdef");
                singleConnection.setMongoAuthDb(authDb);
                singleConnection.setMongoPassword(password);
                singleConnection.setMongoLogin(user);
                singleConnection.setDatabase(morphium.getDatabase());
                log.info("Running test with DB morphium_test_" + number.get() + " for " + singleConnection.getDriverName());
                Morphium m = new Morphium(singleConnection);

                while (running.get()) {
                    log.info("Starting watch...");

                    try {
                        m.watch(UncachedObject.class, false, new ChangeStreamListener() {
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                events.incrementAndGet();
                                return running.get();
                            }
                        });
                    } catch (Exception e) {
                        log.error("Exception from watch: " + e.getMessage());
                    }

                    log.info("Watch ended...");
                }
                m.close();
            }).start();
            String originalConnectedTo = "";
            Thread.sleep(4000);

            for (int i = 0; i < 1000; i++) {
                if (i == 500) {
                    Thread.sleep(100);
                    assertEquals(500, events.get());
                    originalConnectedTo = shutdownPrimary(morphium);
                }

                try {
                    morphium.store(new UncachedObject("Str" + i, i));
                } catch (Exception el) {
                    log.error("Got an error: " + el.getMessage(), el);
                }
                Thread.sleep(10);
            }

            long countAll = morphium.createQueryFor(UncachedObject.class).countAll();
            log.info("Wrote 1000 documents, now in db " + countAll + " got Events: " + events.get());
            assertTrue(countAll - 100 < events.get());
            Thread.sleep(1000);
            checkCluster(morphium, originalConnectedTo);
        }
    }

    private void checkCluster(Morphium morphium, String originalConnectedTo) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("---->>> restart mongod on " + originalConnectedTo + " and press ENTER");
        br.readLine();
        log.info("Node should be started up... checking cluster");
        var drv = morphium.getDriver();
OUT:

        while (true) {
            Thread.sleep(1000);

            try {
                var con = drv.getPrimaryConnection(null);
                ReplicastStatusCommand rstat = new ReplicastStatusCommand(con);
                var status = rstat.execute();
                drv.releaseConnection(con);
                var members = ((List<Map<String, Object>>) status.get("members"));

                for (Map<String, Object> mem : members) {
                    if (mem.get("name").equals(originalConnectedTo)) {
                        Object stateStr = mem.get("stateStr");
                        log.info("Status of original host: " + stateStr);

                        if (stateStr.equals("PRIMARY")) {
                            log.info("Finally...");
                            break OUT;
                        } else if (stateStr.equals("STARTUP") || stateStr.equals("STARTUP2") || stateStr.equals("SECONDARY")) {
                            log.info("Status is still " + stateStr);
                        } else {
                            log.warn("Unknown status " + stateStr);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("retrying due Error during rs-status check: " + e.getMessage());
                Thread.sleep(1000);
            }
        }

        log.info("State recovered");
    }
}
