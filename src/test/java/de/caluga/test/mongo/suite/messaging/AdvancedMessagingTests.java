package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AdvancedMessagingTests extends MorphiumTestBase {
    private final Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @Test
    public void testExclusiveXTimes() throws Exception {
        for (int i = 0; i < 5; i++)
            runExclusiveMessagesTest((int) (Math.random() * 2500+150), (int) (22 * Math.random()) + 2);
    }

    private void runExclusiveMessagesTest(int amount, int receivers) throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(10000, "MsgClass not deleted", ()->!morphium.exists(Msg.class));
        List<Morphium> morphiums = new ArrayList<>();
        List<Messaging> messagings = new ArrayList<>();

        Messaging sender;
        sender = new Messaging(morphium, 50, false);
        sender.setSenderId("amsender");
        AtomicBoolean error=new AtomicBoolean(false);
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
                log.info("Creating morphiums..." + i+"/"+receivers);
                MorphiumConfig cfg2= MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                cfg2.setCredentialsEncryptionKey("1234567890abcdef");
                cfg2.setCredentialsDecryptionKey("1234567890abcdef");

                Morphium m = new Morphium(cfg2);
                m.getConfig().getCache().setHouskeepingIntervalPause(100);
                morphiums.add(m);
                Messaging msg = new Messaging(m, 50, false, true, (int) (1500 * Math.random()));
                msg.setSenderId("msg" + i);
                msg.setUseChangeStream(true).start();
                messagings.add(msg);
                msg.addListenerForMessageNamed("test", msgMessageListener);
            }


            for (int i = 0; i < amount; i++) {
                if (i % 100 == 0) {
                    log.info("Sending message " + i + "/" + amount);
                    log.info("           :  OPEN BORROWED IN_USE");
                    log.info("Sender     : "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_OPENED)+" "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED)+" "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_USE));
                    for (int j =0;j<receivers;j++){
                        log.info("Morphium "+j+": "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_OPENED)+" "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED)+" "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_USE));
                    }
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
                log.info("           :  OPEN BORROWED IN_USE");
                log.info("Sender     : "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_OPENED)+" "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED)+" "+morphium.getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_USE));
                for (int j =0;j<receivers;j++){
                    log.info("Morphium "+j+": "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_OPENED)+" "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_BORROWED)+" "+morphiums.get(j).getDriver().getDriverStats().get(DriverStatsKey.CONNECTIONS_IN_USE));
                }
                for (MorphiumId id : counts.keySet()) {
                    assert (counts.get(id) <= 1) : "Count for id " + id.toString() + " is " + counts.get(id);
                }
                Thread.sleep(2000);
                assertNotEquals(lastCount,counts.size());
                log.info("----> current speed: " + (counts.size() - lastCount) + "/sec");
                lastCount = counts.size();
                assertFalse(error.get(), "An error occured during message processing");
            }
            log.info("-----> Messages processed so far: " + counts.size() + "/" + amount + " with " + receivers + " receivers");
        } finally {
            List<Thread> threads = new ArrayList<>();
            threads.add(new Thread() {
                private Messaging msg;

                public Thread setMessaging(Messaging m) {
                    this.msg = m;
                    return this;
                }

                public void run() {
                    msg.terminate();
                }
            }.setMessaging(sender));
            threads.get(0).start();

            sender.terminate();
            for (Messaging m : messagings) {
                Thread t = new Thread() {
                    private Messaging msg;

                    public Thread setMessaging(Messaging m) {
                        this.msg = m;
                        return this;
                    }

                    public void run() {
                        log.info("Terminating " + m.getSenderId());
                        msg.terminate();
                    }
                }.setMessaging(m);
                threads.add(t);
                t.start();
            }

            for (Thread t : threads) {
                t.join();
            }
            threads.clear();
            int num = 0;
            for (Morphium m : morphiums) {
                num++;

                Thread t = new Thread() {
                    private Morphium m;
                    private int n;

                    public Thread setMorphium(Morphium m, int num) {
                        this.m = m;
                        this.n = num;
                        return this;
                    }

                    public void run() {
                        log.info("Terminating Morphium " + n + "/" + morphiums.size());
                        m.close();
                    }
                }.setMorphium(m, num);

                threads.add(t);
                t.start();
//                log.info("Closing morphium..." + num + "/" + morphiums.size());
//                m.close();
            }
            for (Thread t : threads) {
                t.join();
            }
            threads.clear();
            log.info("Run finished!");
        }

    }

    @Test
    public void messageAnswerTest() throws Exception {
        morphium.dropCollection(Msg.class, "msg", null);
        counts.clear();
        Messaging m1 = new Messaging(morphium, 100, false, true, 10);
        m1.start();


        MorphiumConfig cfg2= MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg2.setCredentialsEncryptionKey("1234567890abcdef");
        cfg2.setCredentialsDecryptionKey("1234567890abcdef");
        Morphium morphium2 = new Morphium(cfg2);
        Messaging m2 = new Messaging(morphium2, 100, false, true, 10);
//        m2.setUseChangeStream(false);
        m2.start();

        cfg2= MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg2.setCredentialsEncryptionKey("1234567890abcdef");
        cfg2.setCredentialsDecryptionKey("1234567890abcdef");
        Morphium morphium3 = new Morphium(cfg2);
        Messaging m3 = new Messaging(morphium3, 100, false, true, 10);
//        m3.setUseChangeStream(false);
        m3.start();

        cfg2= MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg2.setCredentialsEncryptionKey("1234567890abcdef");
        cfg2.setCredentialsDecryptionKey("1234567890abcdef");
        Morphium morphium4 = new Morphium(cfg2);
        Messaging m4 = new Messaging(morphium4, 100, false, true, 10);
//        m4.setUseChangeStream(false);
        m4.start();
        MessageListener<Msg> msgMessageListener = (msg, m) -> {
            log.info("Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
            Msg answer = m.createAnswerMsg();
            answer.setName("test_answer");
            return answer;
        };

        try {
            m2.addListenerForMessageNamed("test", msgMessageListener);
            m3.addListenerForMessageNamed("test", msgMessageListener);
            m4.addListenerForMessageNamed("test", msgMessageListener);

            for (int i = 0; i < 10; i++) {
                log.info("Sending exclusive message "+i+"/10");
                Msg query = new Msg("test", "test query", "query");
                query.setExclusive(true);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 500);
                //            for(Msg m:ans){
                //                log.info("Incoming message: "+m);
                //            }
                assert (ans.size() == 1) : "Recieved more than one answer to query " + query.getMsgId() + " " + ans.size();
            }


            for (int i = 0; i < 10; i++) {
                log.info("Sending non exclusive message "+i+"/10");
                Msg query = new Msg("test", "test query", "query");
                query.setExclusive(false);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 500);
                assert (ans.size() == 3) : "Recieved not enough answers to  " + query.getMsgId();
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();

            morphium2.close();
            morphium3.close();
            morphium4.close();
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
        Thread.sleep(2500);
        assert (counts.get(msgId).equals(1));
        producer.terminate();
        consumer.terminate();

    }
}
