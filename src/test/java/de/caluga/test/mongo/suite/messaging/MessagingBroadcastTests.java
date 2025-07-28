package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.caluga.morphium.messaging.StdMessaging;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

public class MessagingBroadcastTests extends MultiDriverTestBase {

    private boolean gotMessage1, gotMessage2, gotMessage3, gotMessage4, error;
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {
            }
            .getClass().getEnclosingMethod().getName();
            log.info(String.format("=====================> Running Test %s with %s <===============================", method, morphium.getDriver().getName()));
            morphium.clearCollection(Msg.class);
            final StdMessaging m1 = new StdMessaging(morphium, 1000, true, 10);
            final StdMessaging m2 = new StdMessaging(morphium, 10, true, 10);
            final StdMessaging m3 = new StdMessaging(morphium, 10, true, 10);
            final StdMessaging m4 = new StdMessaging(morphium, 10, true, 10);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            error = false;
            m4.start();
            m1.start();
            m3.start();
            m2.start();
            Thread.sleep(1300);

            try {
                log.info("m1 ID: " + m1.getSenderId());
                log.info("m2 ID: " + m2.getSenderId());
                log.info("m3 ID: " + m3.getSenderId());
                m1.addMessageListener((msg, m) -> {
                    gotMessage1 = true;

                    if (m.getTo() != null && m.getTo().contains(m1.getSenderId())) {
                        log.error("wrongly received message m1?");
                        error = true;
                    }
                    log.info("M1 got message " + m.toString());
                    return null;
                });
                m2.addMessageListener((msg, m) -> {
                    gotMessage2 = true;

                    if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                        log.error("wrongly received message m2?");
                        error = true;
                    }
                    log.info("M2 got message " + m.toString());
                    return null;
                });
                m3.addMessageListener((msg, m) -> {
                    gotMessage3 = true;

                    if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                        log.error("wrongly received message m3?");
                        error = true;
                    }
                    log.info("M3 got message " + m.toString());
                    return null;
                });
                m4.addMessageListener((msg, m) -> {
                    gotMessage4 = true;

                    if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                        log.error("wrongly received message m4?");
                        error = true;
                    }
                    log.info("M4 got message " + m.toString());
                    return null;
                });
                Msg m = new Msg("test", "A message", "a value");
                m.setExclusive(false);
                m1.sendMessage(m);

                while (!gotMessage2 || !gotMessage3 || !gotMessage4) {
                    Thread.sleep(500);
                }

                assert(!gotMessage1) : "Got message again?";
                assert(gotMessage4) : "m4 did not get msg?";
                assert(gotMessage2) : "m2 did not get msg?";
                assert(gotMessage3) : "m3 did not get msg";
                assert(!error);
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;
                Thread.sleep(500);
                assert(!gotMessage1) : "Got message again?";
                assert(!gotMessage2) : "m2 did get msg again?";
                assert(!gotMessage3) : "m3 did get msg again?";
                assert(!gotMessage4) : "m4 did get msg again?";
                assert(!error);
            } finally {
                m1.terminate();
                m2.terminate();
                m3.terminate();
                m4.terminate();
            }

            log.info(method + "() finished with " + morphium.getDriver().getName());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void broadcastMultiTest(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {
            }
            .getClass().getEnclosingMethod().getName();
            log.info(String.format("=====================> Running Test %s with %s <===============================", method, morphium.getDriver().getName()));
            StdMessaging sender = new StdMessaging(morphium, 10000, true, 1);
            sender.setSenderId("sender");
            sender.start();
            Map<String, List<MorphiumId >> receivedIds = new ConcurrentHashMap<String, List<MorphiumId >> ();
            List<StdMessaging> receivers = new ArrayList<StdMessaging>();

            for (int i = 0; i < 10; i++) {
                StdMessaging rec1 = new StdMessaging(morphium, 10, true, 10);
                rec1.setSenderId("rec" + i);
                receivers.add(rec1);
                rec1.start();
                rec1.addListenerForMessageNamed("bcast", (msg, m) -> {
                    synchronized(receivedIds) {
                        receivedIds.putIfAbsent(rec1.getSenderId(), new ArrayList<MorphiumId>());

                        if (receivedIds.get(rec1.getSenderId()).contains(m.getMsgId())) {
                            log.error("Duplicate processing!!!!");
                        } else if (m.isExclusive() && m.getProcessedBy() != null && m.getProcessedBy().size() != 0) {
                            log.error("Duplicate processing Exclusive Message!!!!!");
                        }

                        receivedIds.get(rec1.getSenderId()).add(m.getMsgId());
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

                if (totalNum >= amount * receivers.size() + amount) {
                    break;
                } else {
                    log.info("Did not receive all: {} of {}", totalNum, amount * receivers.size() + amount);
                }

                Thread.sleep(200);
                assertTrue(System.currentTimeMillis() - start < 15000, "Did not get all messages in time");
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
                    var m = morphium.findById(Msg.class, e.getValue().get(i));

                    if (m == null) {
                        log.warn("Hmm.. Did not get message... retrying...");
                        Thread.sleep(100);
                        m = morphium.findById(Msg.class, e.getValue().get(i));
                        assertNotNull(m, "Message not found");
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

            for (StdMessaging m : receivers) {
                m.terminate();
            }
            sender.terminate();
            log.info(method + "() finished with " + morphium.getDriver().getName());
        }
    }


}
