package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdvancedInMemMessagingTests extends MorphiumInMemTestBase {
    private Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @Test
    public void testExclusiveMessages() throws Exception {
        counts.clear();
        Messaging m1 = new Messaging(morphium, 10, false, false, 10);
        m1.setUseChangeStream(true);
        m1.start();

        Messaging m2 = new Messaging(morphium, 10, false, false, 10);
        m2.setUseChangeStream(true);
        m2.start();

        Messaging m3 = new Messaging(morphium, 10, false, false, 10);
        m3.setUseChangeStream(true);
        m3.start();

        Messaging m4 = new Messaging(morphium, 10, false, false, 10);
        m4.setUseChangeStream(false);
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

        for (int i = 0; i < 200; i++) {
            Msg m = new Msg("test", "test msg", "value");
            m.setMsgId(new MorphiumId());
            m.setExclusive(true);
            m1.storeMessage(m);

        }

        while (counts.size() < 200) {
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

    }

    @Test
    public void answerWithDifferentNameTest() throws Exception {
        counts.clear();
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        consumer.addListenerForMessageNamed("test", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("incoming message, replying with answer");
                Msg answer = m.createAnswerMsg();
                answer.setName("answer");
                return answer;
            }
        });

        Msg answer = producer.sendAndAwaitFirstAnswer(new Msg("test", "query", "value"), 1000);
        assert (answer != null);
        assert (answer.getName().equals("answer"));


    }

    @Test
    public void ownAnsweringHandler() throws Exception {
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        consumer.addListenerForMessageNamed("test", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("incoming message, replying with answer");
                Msg answer = m.createAnswerMsg();
                answer.setName("answer");
                return answer;
            }
        });

        MorphiumId msgId = new MorphiumId();


        producer.addListenerForMessageNamed("answer", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.info("Incoming answer! " + m.getInAnswerTo() + " ---> " + msgId);
                assert (msgId.equals(m.getInAnswerTo()));
                counts.put(msgId, 1);
                return null;
            }
        });

        Msg msg = new Msg("test", "query", "value");
        msg.setMsgId(msgId);
        producer.storeMessage(msg);
        Thread.sleep(1000);
        assert (counts.get(msgId).equals(1));

    }
}
