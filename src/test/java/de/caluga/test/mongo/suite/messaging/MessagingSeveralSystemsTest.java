package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

public class MessagingSeveralSystemsTest extends MultiDriverTestBase {
    private boolean gotMessage1, gotMessage2, gotMessage3, gotMessage4, error;
    //TODO: Move
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void severalSystemsTest(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {
            }

            .getClass().getEnclosingMethod().getName();
            log.info(String.format("=====================> Running Test %s with %s <===============================", method, morphium.getDriver().getName()));
            morphium.clearCollection(Msg.class);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            error = false;
            final Messaging m1 = new Messaging(morphium, 10, true);
            final Messaging m2 = new Messaging(morphium, 10, true);
            final Messaging m3 = new Messaging(morphium, 10, true);
            final Messaging m4 = new Messaging(morphium, 10, true);
            m4.start();
            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(2000);
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("M1 got message " + m.toString());
                return null;
            });
            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("M2 got message " + m.toString());
                return null;
            });
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                log.info("M3 got message " + m.toString());
                return null;
            });
            m4.addMessageListener((msg, m) -> {
                gotMessage4 = true;
                log.info("M4 got message " + m.toString());
                return null;
            });
            m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
            long start = System.currentTimeMillis();

            while (!gotMessage2 || !gotMessage3 || !gotMessage4) {
                Thread.sleep(1000);
                long dur = System.currentTimeMillis() - start;
                assertTrue(dur < 10000, "Did not get messages");
                log.info("Still waiting for messages...");
            }

            assertTrue(gotMessage2, "Message not recieved yet by m2?!?!?");
            assertTrue(gotMessage3, "Message not recieved yet by m3?!?!?");
            assertTrue(gotMessage4, "Message not recieved yet by m4?!?!?");
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));
            start = System.currentTimeMillis();

            while (!gotMessage1 || !gotMessage3 || !gotMessage4) {
                Thread.sleep(1000);
                long dur = System.currentTimeMillis() - start;
                assertTrue(dur < 10000, "did not get messages");
                log.info("Still waiting for messages...");
            }

            assertTrue(gotMessage1, "Message not recieved yet by m1?!?!?");
            assertTrue(gotMessage3, "Message not recieved yet by m3?!?!?");
            assertTrue(gotMessage4, "Message not recieved yet by m4?!?!?");
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            m1.sendMessage(new Msg("testmsg_excl", "This is the message", "value", 30000000, true));
            start = System.currentTimeMillis();

            while (!gotMessage1 && !gotMessage2 && !gotMessage3 && !gotMessage4) {
                Thread.sleep(1000);
                long dur = System.currentTimeMillis() - start;
                assertTrue(dur < 10000, "did not get messages");
                log.info("Still waiting for messages...");
            }

            int cnt = 0;

            if (gotMessage1) {
                cnt++;
            }

            if (gotMessage2) {
                cnt++;
            }

            if (gotMessage3) {
                cnt++;
            }

            if (gotMessage4) {
                cnt++;
            }

            assertTrue(cnt != 0, "Message was not received");
            assertTrue(cnt == 1, "Message was received too often: " + cnt);
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
            log.info(method + "() finished with " + morphium.getDriver().getName());
        }
    }
}
