package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AnsweringLoadTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void severalMessagingsAnsweringLoadTestSingleMorphium(Morphium morphium) throws Exception {
        try (morphium) {
//            morphium.getConfig().setHeartbeatFrequency(10000);
//            morphium.getDriver().setHeartbeatFrequency(10000);
            morphium.getConfig().setMaxConnections(25);
            morphium.getDriver().setMaxConnectionsPerHost(25);
            morphium.getConfig().setMinConnections(25);
            morphium.getDriver().setMinConnectionsPerHost(25);
            morphium.getConfig().setMaxConnectionIdleTime(60000);
            morphium.getDriver().setMaxConnectionIdleTime(60000);
            morphium.getDriver().setMaxConnectionLifetime(1200000);
            morphium.getConfig().setMaxConnectionLifeTime(1200000);
            AtomicInteger messagesSent = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger answersReceived = new AtomicInteger();
            AtomicInteger messagesReceived = new AtomicInteger();
            AtomicLong runtimeTotal = new AtomicLong();


            long times[] = new long[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
            AtomicInteger tidx = new AtomicInteger();
            var amount = 100;
            var recipients = 3;
            var senderThreads = 20;
            var listener = new MessageListener<Msg>() {
                public Msg onMessage(Messaging m, Msg msg) {
                    messagesReceived.incrementAndGet();
                    long tm = Long.valueOf(msg.getValue());
//                    log.info(m.getSenderId()+": Received after "+(System.currentTimeMillis()-tm)+" - "+msg.getMsgId());
                    Msg answer = msg.createAnswerMsg();
                    answer.setMapValue(Doc.of("answer", System.currentTimeMillis()));
                    answer.setPriority(msg.getPriority() - 10);
                    answer.setTimingOut(false);
                    answer.setDeleteAfterProcessing(true);
                    answer.setDeleteAfterProcessingTime(0);
//                    try {
//                        Thread.sleep(250);
//                    } catch (InterruptedException e) {
//                    }
                    return answer;
                }
            };
            var rec = new ArrayList<Messaging>();
            morphium.getConfig().setIdleSleepTime(10);

            for (int i = 0; i < recipients; i++) {
                log.info("Connection " + i + "/" + recipients);
                var m2 = TestUtils.newMorphiumFrom(morphium);
//                var m2 = morphium; //TestUtils.newMorphiumFrom(morphium);
                log.info("... messaging");
                Messaging m = new Messaging(m2);
                m.setUseChangeStream(true);
                m.setSenderId("Rec" + i);
                m.setWindowSize(120);
                m.setPause(10);
                m.setMultithreadded(true);
                m.setProcessMultiple(true);
                m.addListenerForMessageNamed("test", listener);
                m.start();
                rec.add(m);
            }

            log.info("Waiting for messagings to initialise...");
            Thread.sleep(2000); //waiting for messagings to initialize
            var threads = new ArrayList<Thread>();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            } //waiting for init to be complete
            for (int t = 0; t < senderThreads; t++) {
                Thread thr = new Thread(() -> {
                    var sender = new Messaging(morphium);
                    // var sender = new Messaging(TestUtils.newMorphiumFrom(morphium));
                    sender.setWindowSize(100);
                    sender.setMultithreadded(true);
                    sender.setProcessMultiple(true);
                    sender.setPause(10);
                    sender.start();
                    //warm-up
                    for (int i = 0; i < 5; i++) {
                        Msg m = new Msg("test", "msg", "" + System.currentTimeMillis(), 40000, true);
                        m.setTimingOut(false);

                        m.setDeleteAfterProcessing(true);
                        m.setDeleteAfterProcessingTime(0);
                        sender.sendAndAwaitFirstAnswer(m, 10000, false);
                    }

                    for (int i = 0; i < amount; i++) {
                        // try {
                        //     Thread.sleep((long)(Math.random()*500));
                        // } catch (InterruptedException e) {
                        //     // TODO Auto-generated catch block
                        // }
                        Msg m = new Msg("test", "msg", "value", 40000, true);
                        m.setTimingOut(false);
                        m.setDeleteAfterProcessing(true);
                        m.setDeleteAfterProcessingTime(0);
                        long start = System.currentTimeMillis();
                        m.setValue("" + start);
                        var a = sender.sendAndAwaitFirstAnswer(m, 20000, false);
                        long dur = System.currentTimeMillis() - start;
                        times[tidx.get()] = dur;

                        if (tidx.incrementAndGet() >= times.length) {
                            tidx.set(0);
                        }

                        // log.info("===> got answer after: "+dur);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                        }

                        messagesSent.incrementAndGet();

                        if (a == null) {
                            errors.incrementAndGet();
                        } else {
                            answersReceived.incrementAndGet();
                            runtimeTotal.addAndGet(dur);
                        }
                    }
                    sender.terminate();

                });
                thr.start();
                threads.add(thr);
            }

            AtomicBoolean running = new AtomicBoolean(true);
            Thread loggerThread = new Thread(() -> {
                while (running.get()) {
                    double averageAnswerTime = ((double) runtimeTotal.get()) / ((double) answersReceived.get());
                    long movingAverageTm = 0;
                    int cnt = 0;

                    for (long ti : times) {
                        if (ti >= 0) {
                            cnt++;
                            movingAverageTm += ti;
                        }
                    }

                    double movingAverage = (double) movingAverageTm / (double) times.length;
                    log.info("Sent: " + messagesSent.get() + " Received: " + answersReceived.get() + "---> moving average : " + movingAverage + "   ---> current average: " + averageAnswerTime);

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                }
            });
            loggerThread.start();
            long start = System.currentTimeMillis();

            for (Thread t : threads) {
                t.join();
            }

            running.set(false);
            loggerThread.join();
            long dur = System.currentTimeMillis() - start;

            for (Messaging m : rec) {
                new Thread(() -> {
                    m.terminate();
                });
            }

            Thread.sleep(5000);
            double averageAnswerTime = ((double) runtimeTotal.get()) / ((double) answersReceived.get());
            log.info("Messages sent      : " + messagesSent.get());
            log.info("Messages received  : " + messagesReceived.get());
            log.info("Messages answered  : " + answersReceived.get());
            log.info("average answertime : " + averageAnswerTime);
            log.info("errors             : " + errors.get());
            log.info("Duration for all messages: " + dur);
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void connectionPoolTest(Morphium morphium) throws Exception {
        int threads = 350;
        morphium.getDriver().setMaxConnections(10);
        morphium.getDriver().setMaxConnectionsPerHost(10);
        morphium.getDriver().setMinConnectionsPerHost(1);
        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int thrNum = i;
            var t = new Thread(() -> {
                try {
                    long start = System.currentTimeMillis();
                    var c = morphium.getDriver().getPrimaryConnection(null);
                    long dur = System.currentTimeMillis() - start;
                    log.info(thrNum + ": Getting: " + dur);
                    Thread.sleep(10);
                    c.getDriver().releaseConnection(c);
                    log.info(thrNum + "release: " + (System.currentTimeMillis() - start - 10));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
            thr.add(t);
        }

        for (Thread t : thr) {
            t.join();
        }
        morphium.close();
    }
}
