package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdvancedMessagingTests extends MorphiumTestBase {
    private Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @Test
    public void testExclusiveMessages() throws Exception {
        counts.clear();
        Messaging m1 = new Messaging(morphium, 100, false, true, 10);
        m1.start();


        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m2 = new Messaging(morphium2, 100, false, true, 10);
        m2.start();

        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m3 = new Messaging(morphium3, 100, false, true, 10);
        m3.start();

        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m4 = new Messaging(morphium4, 100, false, false, 10);
        m4.start();

        MessageListener<Msg> msgMessageListener = new MessageListener<Msg>() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
                counts.putIfAbsent(m.getMsgId(), 0);
                counts.put(m.getMsgId(), counts.get(m.getMsgId()) + 1);
                Thread.sleep(100);
                return null;
            }
        };

        m2.addListenerForMessageNamed("test", msgMessageListener);
        m3.addListenerForMessageNamed("test", msgMessageListener);
        m4.addListenerForMessageNamed("test", msgMessageListener);

        for (int i = 0; i < 500; i++) {
            Msg m = new Msg("test", "test msg", "value");
            m.setMsgId(new MorphiumId());
            m.setExclusive(true);
            m1.sendMessage(m);

        }

        while (counts.size() < 500) {
            log.info("-----> Messages processed so far: " + counts.size());
            for (MorphiumId id : counts.keySet()) {
                assert (counts.get(id) <= 1) : "Count for id " + id.toString() + " is " + counts.get(id);
            }
            Thread.sleep(1000);
        }

        m1.terminate();
        m2.terminate();
        m3.terminate();
        m4.terminate();

        morphium2.close();
        morphium3.close();
        morphium4.close();
    }


    @Test
    public void messageAnswerTest() throws Exception {
        counts.clear();
        Messaging m1 = new Messaging(morphium, 100, false, true, 10);
        m1.start();


        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m2 = new Messaging(morphium2, 100, false, true, 10);
//        m2.setUseChangeStream(false);
        m2.start();

        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m3 = new Messaging(morphium3, 100, false, true, 10);
//        m3.setUseChangeStream(false);
        m3.start();

        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        Messaging m4 = new Messaging(morphium4, 100, false, true, 10);
//        m4.setUseChangeStream(false);
        m4.start();
        MessageListener<Msg> msgMessageListener = new MessageListener<Msg>() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
                Msg answer = m.createAnswerMsg();
                answer.setName("test_answer");
                return answer;
            }
        };

        m2.addListenerForMessageNamed("test", msgMessageListener);
        m3.addListenerForMessageNamed("test", msgMessageListener);
        m4.addListenerForMessageNamed("test", msgMessageListener);

        for (int i = 0; i < 100; i++) {
            Msg query = new Msg("test", "test querey", "query");
            query.setExclusive(true);
            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 250);
            assert (ans.size() == 1) : "Recieved more than one answer to query " + query.getMsgId();
        }


        for (int i = 0; i < 100; i++) {
            Msg query = new Msg("test", "test querey", "query");
            query.setExclusive(false);
            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 250);
            assert (ans.size() == 3) : "Recieved not enough answers to  " + query.getMsgId();
        }
        m1.terminate();
        m2.terminate();
        m3.terminate();
        m4.terminate();
    }

    @Test
    public void answerWithDifferentNameTest() throws Exception {
        counts.clear();
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        consumer.addListenerForMessageNamed("testDiff", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("incoming message, replying with answer");
                Msg answer = m.createAnswerMsg();
                answer.setName("answer");
                return answer;
            }
        });

        Msg answer = producer.sendAndAwaitFirstAnswer(new Msg("testDiff", "query", "value"), 1000);
        assert (answer != null);
        assert (answer.getName().equals("answer")) : "Name is wrong: " + answer.getName();
        producer.terminate();
        consumer.terminate();

    }

    @Test
    public void ownAnsweringHandler() throws Exception {
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        consumer.addListenerForMessageNamed("testAnswering", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("incoming message, replying with answer");
                Msg answer = m.createAnswerMsg();
                answer.setName("answerForTestAnswering");
                return answer;
            }
        });

        MorphiumId msgId = new MorphiumId();


        producer.addListenerForMessageNamed("answerForTestAnswering", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("Incoming answer! " + m.getInAnswerTo() + " ---> " + msgId);
                assert (msgId.equals(m.getInAnswerTo()));
                counts.put(msgId, 1);
                return null;
            }
        });

        Msg msg = new Msg("testAnswering", "query", "value");
        msg.setMsgId(msgId);
        producer.sendMessage(msg);
        Thread.sleep(1000);
        assert (counts.get(msgId).equals(1));
        producer.terminate();
        consumer.terminate();

    }
}
