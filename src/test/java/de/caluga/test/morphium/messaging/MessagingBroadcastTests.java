package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.MorphiumMessaging;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;

@Tag("messaging")
public class MessagingBroadcastTests extends MultiDriverTestBase {

    private boolean gotMessage1, gotMessage2, gotMessage3, gotMessage4, error;
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {
            }
            .getClass().getEnclosingMethod().getName();
            morphium.clearCollection(Msg.class);
            for (String msgImpl : de.caluga.test.mongo.suite.base.MorphiumTestBase.messagingsToTest) {
                log.info(String.format("=====================> Running Test %s with Driver %s and Messaging %s <===============================", method, morphium.getDriver().getName(), msgImpl));
                var cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());


                try (Morphium m = new Morphium(cfg)) {
                    // wait until a primary connection is available to avoid race conditions on startup
                    // Important: always release the acquired connection back to the pool
                    de.caluga.test.mongo.suite.base.TestUtils.waitForConditionToBecomeTrue(10000, "No primary node found", () -> {
                        try {
                            var con = m.getDriver().getPrimaryConnection(null);
                            boolean ok = con != null && con.isConnected();
                            if (con != null) m.getDriver().releaseConnection(con);
                            return ok;
                        } catch (Exception e) {
                            return false;
                        }
                    });
                    final MorphiumMessaging m1 = m.createMessaging();
                    final MorphiumMessaging m2 = m.createMessaging();
                    final MorphiumMessaging m3 = m.createMessaging();
                    final MorphiumMessaging m4 = m.createMessaging();
                    gotMessage1 = false;
                    gotMessage2 = false;
                    gotMessage3 = false;
                    gotMessage4 = false;
                    error = false;
                    m4.start();
                    m1.start();
                    m3.start();
                    m2.start();
                    Thread.sleep(1250);

                    try {
                        log.info("m1 ID: " + m1.getSenderId());
                        log.info("m2 ID: " + m2.getSenderId());
                        log.info("m3 ID: " + m3.getSenderId());
                        m1.addListenerForTopic("test", (msg, incoming) -> {
                            gotMessage1 = true;

                            if (incoming.getTo() != null && incoming.getTo().contains(m1.getSenderId())) {
                                log.error("wrongly received message m1?");
                                error = true;
                            }
                            log.info("M1 got message " + incoming.toString());
                            return null;
                        });
                        m2.addListenerForTopic("test", (msg, incoming) -> {
                            gotMessage2 = true;

                            if (incoming.getTo() != null && !incoming.getTo().contains(m2.getSenderId())) {
                                log.error("wrongly received message m2?");
                                error = true;
                            }
                            log.info("M2 got message " + incoming.toString());
                            return null;
                        });
                        m3.addListenerForTopic("test", (msg, incoming) -> {
                            gotMessage3 = true;

                            if (incoming.getTo() != null && !incoming.getTo().contains(m3.getSenderId())) {
                                log.error("wrongly received message m3?");
                                error = true;
                            }
                            log.info("M3 got message " + incoming.toString());
                            return null;
                        });
                        m4.addListenerForTopic("test", (msg, incoming) -> {
                            gotMessage4 = true;

                            if (incoming.getTo() != null && !incoming.getTo().contains(m3.getSenderId())) {
                                log.error("wrongly received message m4?");
                                error = true;
                            }
                            log.info("M4 got message " + incoming.toString());
                            return null;
                        });
                        Msg msgObj = new Msg("test", "A message", "a value");
                        msgObj.setExclusive(false);
                        m1.sendMessage(msgObj);

                        while (!gotMessage2 || !gotMessage3 || !gotMessage4) {
                            Thread.sleep(500);
                        }

                        org.junit.jupiter.api.Assertions.assertFalse(gotMessage1, "Got message again?");
                        org.junit.jupiter.api.Assertions.assertTrue(gotMessage4, "m4 did not get msg?");
                        org.junit.jupiter.api.Assertions.assertTrue(gotMessage2, "m2 did not get msg?");
                        org.junit.jupiter.api.Assertions.assertTrue(gotMessage3, "m3 did not get msg");
                        org.junit.jupiter.api.Assertions.assertFalse(error);
                        gotMessage2 = false;
                        gotMessage3 = false;
                        gotMessage4 = false;
                        Thread.sleep(500);
                        org.junit.jupiter.api.Assertions.assertFalse(gotMessage1, "Got message again?");
                        org.junit.jupiter.api.Assertions.assertFalse(gotMessage2, "m2 did get msg again?");
                        org.junit.jupiter.api.Assertions.assertFalse(gotMessage3, "m3 did get msg again?");
                        org.junit.jupiter.api.Assertions.assertFalse(gotMessage4, "m4 did get msg again?");
                        org.junit.jupiter.api.Assertions.assertFalse(error);
                    } finally {
                        m1.terminate();
                        m2.terminate();
                        m3.terminate();
                        m4.terminate();
                    }
                }
                log.info(">>>>>>>>>>> Finished test {} with Driver {} and Messaging {} successfully <<<<<<<<<<<<<", method, morphium.getDriver().getName(), msgImpl);
            }

            log.info(method + "() finished with " + morphium.getDriver().getName());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastMultiTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String msgImpl : de.caluga.test.mongo.suite.base.MorphiumTestBase.messagingsToTest) {
                var cfg = morphium.getConfig().createCopy();
                log.info("Running test with {} messaging", msgImpl);
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());


                final AtomicInteger errorCount = new AtomicInteger();
                try (Morphium morph = new Morphium(cfg)) {
                    String method = new Object() {
                    }
                    .getClass().getEnclosingMethod().getName();
                    log.info(String.format("=====================> Running Test %s with %s and Messaging %s <===============================", method, morphium.getDriver().getName(), msgImpl));
                    MorphiumMessaging sender = morph.createMessaging();
                    sender.setSenderId("sender");
                    sender.start();
                    Map<String, List<MorphiumId >> receivedIds = new ConcurrentHashMap<String, List<MorphiumId >> ();
                    List<MorphiumMessaging> receivers = new ArrayList<MorphiumMessaging>();

                    for (int i = 0; i < 10; i++) {
                        MorphiumMessaging rec1 = morph.createMessaging();
                        rec1.setSenderId("rec" + i);
                        receivers.add(rec1);
                        rec1.start();
                        rec1.addListenerForTopic("bcast", (msg, incoming) -> {
                            synchronized(receivedIds) {
                                receivedIds.putIfAbsent(rec1.getSenderId(), new ArrayList<MorphiumId>());

                                if (receivedIds.get(rec1.getSenderId()).contains(incoming.getMsgId())) {
                                    log.error("Duplicate processing by {} using Messaging {}/{}! Message: {}, processed_by: {}",
                                             rec1.getSenderId(), morphium.getDriver().getName(), msgImpl, incoming.getMsgId(), incoming.getProcessedBy());
                                    errorCount.incrementAndGet();
                                } else if (incoming.isExclusive() && incoming.getProcessedBy() != null && incoming.getProcessedBy().size() != 0) {
                                    log.error("Duplicate processing Exclusive Message by {} using Messaging {}! Message: {}",
                                             rec1.getSenderId(), morphium.getDriver().getName(), msgImpl, incoming.getMsgId());
                                    errorCount.incrementAndGet();
                                }

                                receivedIds.get(rec1.getSenderId()).add(incoming.getMsgId());
                            }
                            return null;
                        });
                    }
                    int amount = 100;

                    for (int i = 0; i < amount; i++) {
                        sender.queueMessage(new Msg("bcast", "msg", "value", 40000));
                        sender.queueMessage(new Msg("bcast", "msg", "value", 40000, true));

                        if (i % 10 == 0) {
                            log.info("Sent message #" + i);
                        }
                    }
                    long start = System.currentTimeMillis();
                    long maxTotalTimeout = 60000; // Max 60 seconds total (safety net for slow machines)
                    long idleTimeout = 20000; // 20 seconds after last message received (increased from 10s)
                    long lastMessageTime = System.currentTimeMillis();
                    int lastTotalNum = 0;

                    while (true) {
                        StringBuilder b = new StringBuilder();
                        int totalNum = 0;
                        log.info("-------------");

                        for (var e : receivedIds.entrySet()) {
                            b.setLength(0);
                            b.append(e.getKey());
                            b.append(": ");

                            for (int i = 0; i < e.getValue().size(); i++) {
                                b.append("*");
                            }

                            totalNum += e.getValue().size();
                            b.append(" -> ");
                            b.append(e.getValue().size());
                            log.info(b.toString());
                        }

                        // Update last message time if we received new messages
                        if (totalNum > lastTotalNum) {
                            lastMessageTime = System.currentTimeMillis();
                            lastTotalNum = totalNum;
                        }

                        if (totalNum >= amount * receivers.size() + amount) {
                            break;
                        } else {
                            log.info("Did not receive all: {} of {} using messaging {} and driver {}", totalNum, amount * receivers.size() + amount, msgImpl, morphium.getDriver().getName());
                        }

                        assertEquals(0, errorCount.get(), "There were errors during processing using " + morphium.getDriver().getName() + "/" + msgImpl);
                        Thread.sleep(200);

                        // Check both idle timeout (5s since last message) and max total timeout (30s total)
                        long timeSinceLastMessage = System.currentTimeMillis() - lastMessageTime;
                        long totalTime = System.currentTimeMillis() - start;
                        assertTrue(timeSinceLastMessage < idleTimeout,
                                   "No messages received for " + timeSinceLastMessage + "ms (idle timeout: " + idleTimeout + "ms) using messaging " + msgImpl);
                        assertTrue(totalTime < maxTotalTimeout,
                                   "Total timeout exceeded: " + totalTime + "ms (max: " + maxTotalTimeout + "ms) using messaging " + msgImpl);
                    }
                    Thread.sleep(1000);
                    log.info("--------");
                    StringBuilder b = new StringBuilder();
                    int totalNum = 0;
                    int bcast = 0;
                    int excl = 0;

                    for (var e : receivedIds.entrySet()) {
                        b.setLength(0);
                        b.append(e.getKey());
                        b.append(": ");
                        int ex = 0;
                        int bx = 0;

                        for (int i = 0; i < e.getValue().size(); i++) {
                            var m = morph.findById(Msg.class, e.getValue().get(i), sender.getCollectionName("bcast"));

                            if (m == null) {
                                log.warn("Hmm.. Did not get message... retrying...{}/{}", msgImpl, morphium.getDriver().getName());
                                Thread.sleep(1000);
                                m = morph.findById(Msg.class, e.getValue().get(i), sender.getCollectionName("bcast"));
                                assertNotNull(m, "Message not found using " + msgImpl + "/" + morphium.getDriver().getName());
                            }

                            if (m.isExclusive()) {
                                b.append("!");
                                excl++;
                                ex++;
                            } else {
                                b.append("*");
                                bcast++;
                                bx++;
                            }
                        }

                        totalNum += e.getValue().size();
                        b.append(" -> ");
                        b.append(e.getValue().size());
                        b.append(" ( excl: ");
                        b.append(ex);
                        b.append(" + bcast: ");
                        b.append(bx);
                        b.append(")");
                        log.info(b.toString());
                    }
                    log.info("Total processed: " + totalNum);
                    log.info("    exclusives : " + excl);
                    log.info("    broadcasts : " + bcast);

                    for (MorphiumMessaging m : receivers) {
                        new Thread(()-> m.terminate()).start();
                    }
                    sender.terminate();
                    log.info("{}() finished with {}/{}", method, morphium.getDriver().getName(), msgImpl);
                }
            }
        }

    }
}
