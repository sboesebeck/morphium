package de.caluga.test.mongo.suite.messaging;

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
        m1.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
        m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
        Thread.sleep(100);
        List<Msg> answers = m2.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 1000);

        assert (answers.size() == 0);
        answers = m1.sendAndAwaitAnswers(new Msg("test", "ms", "val"), 10, 1000);
        assert (answers.size() == 0);
        log.info("Messagecount: " + morphium.createQueryFor(Msg.class).countAll());
        assert (morphium.createQueryFor(Msg.class).countAll() == 4);

        m1.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
        m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
        morphium.createQueryFor(Msg.class).delete();
        Thread.sleep(200);
        m1.sendMessage(new Msg("test", "ms", "val"));
        Thread.sleep(500);
        log.info("Messagecount: " + morphium.createQueryFor(Msg.class).countAll());
        assert (morphium.createQueryFor(Msg.class).countAll() == 3) : "Message count is wrong: " + morphium.createQueryFor(Msg.class).countAll();

        //creating a loop!

        m2.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
        morphium.createQueryFor(Msg.class).delete();
        Thread.sleep(200);
        m1.sendMessage(new Msg("test", "ms", "val"));
        Thread.sleep(250);
        m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
        m1.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
        Thread.sleep(250);
        long cnt = morphium.createQueryFor(Msg.class).countAll();
        log.info("Messagecount PingPongLoop: " + cnt);
        assert (cnt > 10);

        assert (cnt == morphium.createQueryFor(Msg.class).countAll());

        m1.terminate();
        m2.terminate();


    }


}
