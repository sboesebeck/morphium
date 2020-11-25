package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import java.util.List;

public class ComplexAnswerTests extends MorphiumTestBase {


    @Test
    public void pingPongTest() throws Exception {
        Messaging m1 = new Messaging(morphium);
        Messaging m2 = new Messaging(morphium);

        m1.start();
        m2.start();


        m1.addListenerForMessageNamed("test", (msg, m) -> m.createAnswerMsg());
        Thread.sleep(100);
        m2.addListenerForMessageNamed("test", (msg, m) -> {
            log.info("Incoming message on m2!");
            return m.createAnswerMsg();
        });
        Thread.sleep(100);
        List<Msg> answers = m2.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 1000);

        assert (answers.size() == 1);
        answers = m1.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 1000);
        assert (answers.size() == 1);
        log.info("MEssages: " + morphium.createQueryFor(Msg.class).countAll());
        assert (morphium.createQueryFor(Msg.class).countAll() == 4);

    }
}
