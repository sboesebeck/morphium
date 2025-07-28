package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RoundTripTests extends MorphiumTestBase {

    List<Long> pingSent = Collections.synchronizedList(new ArrayList<>());
    List<Long> pingReceived = Collections.synchronizedList(new ArrayList<>());
    List<Long> pongSent = Collections.synchronizedList(new ArrayList<>());
    List<Long> pongReceived = Collections.synchronizedList(new ArrayList<>());

    @Test
    public void runSameMorphiumNotExclusive() throws Exception {
        runTest(true, false, true, false, 100, 50);
    }

    @Test
    public void runSameMorphiumExclusive() throws Exception {
        runTest(true, true, true, false, 100, 50);
    }

    @Test
    public void runSeparateMorphiumExclusive() throws Exception {
        runTest(false, true, true, false, 100, 50);
    }

    @Test
    public void runSeparateMorphiumNotExclusive() throws Exception {
        runTest(false, false, true, false, 100, 50);
    }

    public void runTest(boolean sameMorphium, boolean exclusive, boolean multithreadded, boolean processMultiple, int warmUp, int amount) throws Exception {
        log.info("===========> Running test: " + (sameMorphium ? "on same Morphium Instance" : "separate Morphium Instances") + " " + (exclusive ? "exclusive messages" : "not exclusive messages")
            + " " + (processMultiple ? "processing multiple" : "single message processing") + " " + (multithreadded ? "multithreadded" : "single thread"));
        //        morphium.getConfig().setThreadPoolMessagingCoreSize(100);
        //        morphium.getConfig().setThreadPoolMessagingMaxSize(200);
        StdMessaging m1 = new StdMessaging(morphium, 100, processMultiple, multithreadded, 10);
        m1.setSenderId("m1");
        StdMessaging m2;
        Morphium morphium2 = null;

        if (sameMorphium) {
            m2 = new StdMessaging(morphium, 100, processMultiple, multithreadded, 10);
        } else {
            morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
            m2 = new StdMessaging(morphium2, 100, processMultiple, multithreadded, 10);
        }

        m2.setSenderId("m2");

        try {
            m1.start();
            m2.start();
            Thread.sleep(5000);
            m2.addListenerForMessageNamed("ping", (msg, m) -> {
                pingReceived.add(System.currentTimeMillis());
                msg.sendMessage(new Msg("pong", "msg", "v", 30000, exclusive));
                pongSent.add(System.currentTimeMillis());
                return null;
            });
            m1.addListenerForMessageNamed("pong", (msg, m) -> {
                pongReceived.add(System.currentTimeMillis());
                //log.info("got pong back...");
                return null;
            });
            // log.info("warming up...");
            // pingSent.clear();
            // pingReceived.clear();
            // pongSent.clear();
            // pongReceived.clear();
            // long start = System.currentTimeMillis();
            //
            // for (int i = 0; i < warmUp; i++) {
            //     m1.sendMessage(new Msg("ping", "msg", "v", 30000, exclusive));
            //     pingSent.add(System.currentTimeMillis());
            // }
            // while (pongReceived.size() < warmUp) {
            //     log.info("Waiting for pongs... got: " + pongReceived.size() + "/" + warmUp);
            //     Thread.sleep(500);
            // }
            log.info("Starting...");
            pingSent.clear();
            pingReceived.clear();
            pongSent.clear();
            pongReceived.clear();
            var start = System.currentTimeMillis();

            for (int i = 0; i < amount; i++) {
                m1.sendMessage(new Msg("ping", "msg", "v", 30000, false));
                pingSent.add(System.currentTimeMillis());
                Thread.sleep(50);
            }

            while (pongReceived.size() < amount) {
                log.info("Waiting for answers...got: " + pongReceived.size() + "/" + amount);
                Thread.sleep(500);
            }

            Collections.sort(pingSent);
            Collections.sort(pingReceived);
            Collections.sort(pongSent);
            Collections.sort(pongReceived);
            log.info("ping sent      : " + timesListString(start, pingSent));
            log.info("ping received  : " + timesListString(start, pingReceived));
            log.info("ping duration  : " + timesListString(pingReceived, pingSent));
            log.info("pong sent      : " + timesListString(start, pongSent));
            log.info("pong received  : " + timesListString(start, pongReceived));
            log.info("pong duration  : " + timesListString(pongReceived, pongSent));
            StringBuilder b = new StringBuilder();
            int idx = 0;
            long sum = 0;
            long min = 99999999999L;
            long max = 0;

            for (Long t : pingSent) {
                long dur = pongReceived.get(idx) - t;
                sum = sum + dur;

                if (dur > max) {
                    max = dur;
                }

                if (dur < min) {
                    min = dur;
                }

                if (dur < 100) {
                    b.append("  ");
                } else if (dur < 1000) {
                    b.append(" ");
                }

                b.append(dur);
                b.append(", ");
                idx++;
            }

            b.setLength(b.length() - 2);
            log.info("total roundtrip: " + b.toString());
            log.info("avg. roundtrip : " + (sum / pingSent.size()));
            log.info("min  roundtrip : " + min);
            log.info("max  roundtrip : " + max);
        } finally {
            m1.terminate();
            m2.terminate();

            if (morphium2 != null) {
                morphium2.close();
            }
        }
    }

    private String timesListString(long startTimestamp, List<Long> times) {
        StringBuilder b = new StringBuilder();

        for (Long sent : times) {
            long t = sent - startTimestamp;

            if (t < 10) {
                b.append("   ");
            } else if (t < 100) {
                b.append("  ");
            } else if (t < 1000) {
                b.append(" ");
            }

            b.append(t);
            b.append(", ");
        }

        if (b.length() > 2) {
            b.setLength(b.length() - 2);
        }

        return b.toString();
    }

    private String timesListString(List<Long> times, List<Long> substract) {
        StringBuilder b = new StringBuilder();
        int idx = 0;

        for (Long sent : times) {
            if (substract.size() <= idx) break;

            long t = sent - substract.get(idx);

            if (t < 10) {
                b.append("   ");
            } else if (t < 100) {
                b.append("  ");
            } else if (t < 1000) {
                b.append(" ");
            }

            b.append(t);
            b.append(", ");
            idx++;
        }

        b.setLength(b.length() - 2);
        return b.toString();
    }
}
