package de.caluga.test.mongo.suite.ncmessaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
public class AdvancedMessagingNCTests extends MorphiumTestBase {
    private final Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @Test
    public void testExclusiveXTimes() throws Exception {
        //        morphium.watchAsync("msg", true,new ChangeStreamListener(){
        //
        //            @Override
        //            public boolean incomingData(ChangeStreamEvent evt) {
        //
        //                if (evt.getOperationType().equals("insert")){
        //
        //                    storage.put(evt.getDocumentKey(),new ArrayList<>());
        //                    storage.get(evt.getDocumentKey()).add(evt.getFullDocument());
        //
        //                } else if (evt.getOperationType().equals("update")){
        //                    if (evt.getUpdatedFields().containsKey("locked_by")){
        //                            storage.get(evt.getDocumentKey()).add(evt.getFullDocument());
        //
        //                    }
        //                } else if (evt.getOperationType().equals("delete")){
        //                    //storage.remove(evt.getDocumentKey());
        //                }
        //                return true;
        //            }
        //        });
        for (int i = 0; i < 2; i++)
            runExclusiveMessagesTest((int)(Math.random() * 1500), (int)(55 * Math.random()) + 2);
    }

    private void runExclusiveMessagesTest(int amount, int receivers) throws Exception {
        morphium.dropCollection(Msg.class, "msg", null);
        Thread.sleep(1000);
        List<Morphium> morphiums = new ArrayList<>();
        List<StdMessaging> messagings = new ArrayList<>();
        StdMessaging sender = null;
        sender = new StdMessaging(morphium, 50, true, 1);
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
                Morphium m = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
                m.getConfig().getCache().setHouskeepingIntervalPause(100);
                morphiums.add(m);
                StdMessaging msg = new StdMessaging(m, 50,  true, (int)(1500 * Math.random()));
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
                    assert(counts.get(id) <= 1) : "Count for id " + id.toString() + " is " + counts.get(id);
                }

                Thread.sleep(1000);
                assert(counts.size() != lastCount);
                log.info("----> current speed: " + (counts.size() - lastCount) + "/sec");
                lastCount = counts.size();
            }

            log.info("-----> Messages processed so far: " + counts.size() + "/" + amount + " with " + receivers + " receivers");
        } finally {
            List<Thread> threads = new ArrayList<>();
            threads.add(new Thread() {
                private StdMessaging msg;
                public Thread setMessaging(StdMessaging m) {
                    this.msg = m;
                    return this;
                }
                public void run() {
                    msg.terminate();
                }
            } .setMessaging(sender));
            threads.get(0).start();
            sender.terminate();

            for (StdMessaging m : messagings) {
                Thread t = new Thread() {
                    private StdMessaging msg;
                    public Thread setMessaging(StdMessaging m) {
                        this.msg = m;
                        return this;
                    }
                    public void run() {
                        log.info("Terminating " + m.getSenderId());
                        msg.terminate();
                    }
                } .setMessaging(m);
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
                } .setMorphium(m, num);
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
        Thread.sleep(100);
        counts.clear();
        StdMessaging m1 = new StdMessaging(morphium, 100,  true, 1);
        m1.setUseChangeStream(false).start();
        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        StdMessaging m2 = new StdMessaging(morphium2, 100,  true, 1);
        //        m2.setUseChangeStream(false);
        m2.setUseChangeStream(false).start();
        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        StdMessaging m3 = new StdMessaging(morphium3, 100,  true, 1);
        //        m3.setUseChangeStream(false);
        m3.setUseChangeStream(false).start();
        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        StdMessaging m4 = new StdMessaging(morphium4, 100,  true, 1);
        //        m4.setUseChangeStream(false);
        m4.setUseChangeStream(false).start();

        try {
            MessageListener<Msg> msgMessageListener = (msg, m) -> {
                log.info("Received " + m.getMsgId() + " created " + (System.currentTimeMillis() - m.getTimestamp()) + "ms ago");
                Msg answer = m.createAnswerMsg();
                answer.setName("test_answer");
                return answer;
            };
            m2.addListenerForMessageNamed("test", msgMessageListener);
            m3.addListenerForMessageNamed("test", msgMessageListener);
            m4.addListenerForMessageNamed("test", msgMessageListener);

            for (int i = 0; i < 10; i++) {
                Msg query = new Msg("test", "test querey", "query");
                query.setExclusive(true);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 1250);
                assert(ans.size() == 1) : "Recieved more than one answer to query " + query.getMsgId();
            }

            for (int i = 0; i < 10; i++) {
                Msg query = new Msg("test", "test querey", "query");
                query.setExclusive(false);
                List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 1250);
                assert(ans.size() == 3) : "Recieved not enough answers to  " + query.getMsgId();
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
        }
    }
    //
    //
    //    @Test
    //    public void testMorphiums() throws Exception {
    //
    //        final List<Morphium>morphiums=new ArrayList<>();
    //        for (int i=0;i<150;i++) {
    //            Morphium m = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
    //            m.getConfig().getCache().setHouskeepingIntervalPause(100);
    //            morphiums.add(m);
    //        }
    //
    //
    //
    //        final Msg msg=new Msg("name","msg","value");
    //        msg.setSender("test");
    //        msg.setMsgId(new MorphiumId());
    //
    //        final AtomicLong cnt=new AtomicLong();
    //        morphium.store(msg);
    //
    //        Thread.sleep(200);
    //
    //        for (int i =0;i<100;i++) {
    //            cnt.set(0);
    //            msg.setLocked(System.currentTimeMillis());
    //
    //            for (final Morphium m:morphiums) {
    //                new Thread() {
    //                    public void run() {
    //                        while (m.createQueryFor(Msg.class, "msg").f("_id").eq(msg.getMsgId()).get().getLocked() != msg.getLocked()) {
    //                            yield();
    //                        }
    //                        cnt.incrementAndGet();
    //                    }
    //                }.start();
    //            }
    //
    //            long start = System.currentTimeMillis();
    //            morphium.set(msg, "locked", msg.getLocked());
    //            long end=System.currentTimeMillis();
    //            while(cnt.get()<150){
    //                Thread.yield();
    //            }
    //            log.info("Turnaround update        : " + (System.currentTimeMillis() - start));
    //            //log.info("Turnaround update (local): " + (end - start));
    //        }
    //
    //
    //    }

    @Test
    public void answerWithDifferentNameTest() throws Exception {
        counts.clear();
        StdMessaging producer = new StdMessaging(morphium, 100,  true, 1);
        producer.setUseChangeStream(false).start();
        StdMessaging consumer = new StdMessaging(morphium, 100, true, 1);
        consumer.setUseChangeStream(false).start();
        Msg answer;

        try {
            consumer.addListenerForMessageNamed("testDiff", (msg, m) -> {
                log.info("incoming message, replying with answer");
                Msg answer1 = m.createAnswerMsg();
                answer1.setName("answer");
                return answer1;
            });
            answer = producer.sendAndAwaitFirstAnswer(new Msg("testDiff", "query", "value"), 1000);
            assertNotNull(answer);
            ;
            assert(answer.getName().equals("answer")) : "Name is wrong: " + answer.getName();
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }

    @Test
    public void ownAnsweringHandler() throws Exception {
        StdMessaging producer = new StdMessaging(morphium, 100,  true, 1);
        producer.setUseChangeStream(false).start();
        StdMessaging consumer = new StdMessaging(morphium, 100,  true, 1);
        consumer.setUseChangeStream(false).start();

        try {
            consumer.addListenerForMessageNamed("testAnswering", (msg, m) -> {
                log.info("incoming message, replying with answer");
                Msg answer = m.createAnswerMsg();
                answer.setName("answerForTestAnswering");
                return answer;
            });
            MorphiumId msgId = new MorphiumId();
            producer.addListenerForMessageNamed("answerForTestAnswering", (msg, m) -> {
                log.info("Incoming answer! " + m.getInAnswerTo() + " ---> " + msgId);
                assert(msgId.equals(m.getInAnswerTo()));
                counts.put(msgId, 1);
                return null;
            });
            Msg msg = new Msg("testAnswering", "query", "value");
            msg.setMsgId(msgId);
            producer.sendMessage(msg);
            Thread.sleep(1000);
            assert(counts.get(msgId).equals(1));
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }
}
