package de.caluga.test.mongo.suite.inmem_messaging;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AdvancedInMemMessagingTests extends MorphiumInMemTestBase {
    private final Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @Test
    public void testExclusiveXTimes() throws Exception {
        for (int i = 0; i < 2; i++)
            runExclusiveMessagesTest((int) (Math.random() * 3500), (int) (22 * Math.random()) + 2);
    }

    private void runExclusiveMessagesTest(int amount, int receivers) throws Exception {

        List<Messaging> messagings = new ArrayList<>();

        Messaging sender;
        sender = new Messaging(morphium, 50, false);
        sender.setSenderId("amsender");
        try {
            log.info("Running Exclusive message test - sending " + amount + " exclusive messages, received by " + receivers);
            morphium.dropCollection(Msg.class, "msg", null);
            log.info("Collection dropped");

            Thread.sleep(100);
            counts.clear();


            MessageListener<Msg> msgMessageListener = (msg, m) -> {
                //log.info(msg.getSenderId() + ": Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
                counts.putIfAbsent(m.getMsgId(), 0);
                counts.put(m.getMsgId(), counts.get(m.getMsgId()) + 1);
                if (counts.get(m.getMsgId()) > 1) {
                    log.error("Msg: " + m.getMsgId() + " processed: " + counts.get(m.getMsgId()));
                    for (String id : m.getProcessedBy()) {
                        log.error("... processed by: " + id);
                    }
                }

                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                }
                return null;
            };
            for (int i = 0; i < receivers; i++) {
                log.info("Creating morphiums..." + i);

                Messaging msg = new Messaging(morphium, 50, false, true, (int) (1500 * Math.random()));
                msg.setSenderId("msg" + i);
                msg.setUseChangeStream(true).start();
                messagings.add(msg);
                msg.addListenerForMessageNamed("test", msgMessageListener);
            }


            for (int i = 0; i < amount; i++) {
                if (i % 100 == 0) {
                    log.info("Sending message " + i + "/" + amount);
                }
                Msg m = new Msg("test", "test msg" + i, "value" + i);
                m.setMsgId(new MorphiumId());
                m.setExclusive(true);
                m.setTtl(600000);
                sender.sendMessage(m);

            }
            int lastCount = counts.size();
            while (counts.size() < amount) {
                log.info("-----> Messages processed so far: " + counts.size() + "/" + amount + " with " + receivers + " receivers");
                for (MorphiumId id : counts.keySet()) {
                    assert (counts.get(id) <= 1) : "Count for id " + id.toString() + " is " + counts.get(id);
                }
                Thread.sleep(2000);
                assert (counts.size() != lastCount);
                log.info("----> current speed: " + (counts.size() - lastCount) / 2 + "/sec");
                lastCount = counts.size();
            }
            log.info("-----> Messages processed so far: " + counts.size() + "/" + amount + " with " + receivers + " receivers");
        } finally {
            sender.terminate();
            for (Messaging m : messagings) {
                m.terminate();
            }

            log.info("Run finished!");
        }

    }

    @Test
    public void messageAnswerTest() throws Exception {
        morphium.dropCollection(Msg.class, "msg", null);
        counts.clear();
        TestUtils.waitForConditionToBecomeTrue(1000, "Message collection did not drop", ()->!morphium.exists(Msg.class));
        log.info("Starting m1...");
        Messaging m1 = new Messaging(morphium, 100, false, true, 10);
        m1.setSenderId("m1");
        m1.start();


        log.info("Starting m2...");
        Messaging m2 = new Messaging(morphium, 100, false, true, 10);
        m2.setSenderId("m2");
        m2.setUseChangeStream(false);

        log.info("Starting m3...");
        Messaging m3 = new Messaging(morphium, 100, false, true, 10);
        m3.setSenderId("m3");
       m3.setUseChangeStream(false);

        log.info("Starting m4...");
        Messaging m4 = new Messaging(morphium, 100, false, true, 10);
        m4.setSenderId("m4");
       m4.setUseChangeStream(false);
        MessageListener<Msg> msgMessageListener = (msg, m) -> {
            log.info("Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
            Msg answer = m.createAnswerMsg();
            answer.setName("test_answer");
            return answer;
        };
        
        m2.addListenerForMessageNamed("test", msgMessageListener);
        m2.start();
        m3.addListenerForMessageNamed("test", msgMessageListener);
        m3.start();
        m4.addListenerForMessageNamed("test", msgMessageListener);
        m4.start();
        Thread.sleep(1000);
        try {

            for (int i = 0; i < 10; i++) {
                log.info("Sending exclusive message");
                Msg query = new Msg("test", "test query", "query");
                query.setExclusive(true);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 2500);
                //            for(Msg m:ans){
                //                log.info("Incoming message: "+m);
                //            }
                assertEquals(1,ans.size());
            }


            for (int i = 0; i < 10; i++) {
                log.info("Sending not exclusive message");
                Msg query = new Msg("test", "test query", "query");
                query.setExclusive(false);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 1000);
                assertEquals(3,ans.size());// : "Recieved not enough answers to  " + query.getMsgId();
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
        }

    }

    @Test
    public void answerWithDifferentNameTest() throws Exception {
        counts.clear();
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        try {
            consumer.addListenerForMessageNamed("testDiff", new MessageListener() {
                @Override
                public Msg onMessage(Messaging msg, Msg m)  {
                    log.info("incoming message, replying with answer");
                    Msg answer = m.createAnswerMsg();
                    answer.setName("answer");
                    return answer;
                }
            });

            Msg answer = producer.sendAndAwaitFirstAnswer(new Msg("testDiff", "query", "value"), 1000);
            assertNotNull(answer);
            ;
            assert (answer.getName().equals("answer")) : "Name is wrong: " + answer.getName();
        } finally {
            producer.terminate();
            consumer.terminate();
        }


    }

    @Test
    public void ownAnsweringHandler() throws Exception {
        Messaging producer = new Messaging(morphium, 100, false, true, 10);
        producer.start();
        Messaging consumer = new Messaging(morphium, 100, false, true, 10);
        consumer.start();

        consumer.addListenerForMessageNamed("testAnswering", (msg, m) -> {
            log.info("incoming message, replying with answer");
            Msg answer = m.createAnswerMsg();
            answer.setName("answerForTestAnswering");
            return answer;
        });

        MorphiumId msgId = new MorphiumId();


        producer.addListenerForMessageNamed("answerForTestAnswering", (msg, m) -> {
            log.info("Incoming answer! " + m.getInAnswerTo() + " ---> " + msgId);
            assert (msgId.equals(m.getInAnswerTo()));
            counts.put(msgId, 1);
            return null;
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
