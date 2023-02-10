package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

public class ComplexAnswerTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void pingPongTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging m1 = new Messaging(morphium);
            Messaging m2 = new Messaging(morphium);
            m1.start();
            m2.start();

            try {
                m1.addListenerForMessageNamed("test", (msg, m)->m.createAnswerMsg());
                Thread.sleep(100);
                m2.addListenerForMessageNamed("test", (msg, m)->{
                    log.info("Incoming message on m2!");
                    return m.createAnswerMsg();
                });
                m1.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
                m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
                Thread.sleep(1000);
                List<Msg> answers = m2.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 5000);
                assert(answers.size() == 1);
                answers = m1.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 5000);
                assert(answers.size() == 1);
                log.info("Messagecount: " + morphium.createQueryFor(Msg.class).countAll());
                assert(morphium.createQueryFor(Msg.class).countAll() == 4);
                m1.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
                m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
                morphium.createQueryFor(Msg.class).delete();
                Thread.sleep(200);
                m1.sendMessage(new Msg("test", "ms", "val"));
                long s = System.currentTimeMillis();

                while (morphium.createQueryFor(Msg.class).countAll() != 3) {
                    Thread.sleep(500);
                    assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
                }

                log.info("Messagecount: " + morphium.createQueryFor(Msg.class).countAll());
                assert(morphium.createQueryFor(Msg.class).countAll() == 3) : "Message count is wrong: " + morphium.createQueryFor(Msg.class).countAll();
                //creating a loop!
                m2.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
                m1.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
                morphium.createQueryFor(Msg.class).delete();
                Thread.sleep(200);
                m1.sendMessage(new Msg("test", "ms", "val"));
                long cnt = morphium.createQueryFor(Msg.class).countAll();
                s = System.currentTimeMillis();

                while (cnt <= 10) {
                    cnt = morphium.createQueryFor(Msg.class).countAll();
                    Thread.sleep(100);
                    assert(System.currentTimeMillis() - s < 20*morphium.getConfig().getMaxWaitTime()); //inMem is quite slow
                }

                log.info("Messagecount PingPongLoop: " + cnt);
                assertTrue(cnt >= 10,"Pingpong delivered not enough messages: " + cnt);
            } finally {
                m1.terminate();
                m2.terminate();
            }
        }
    }

}
