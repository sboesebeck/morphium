package de.caluga.test.mongo.suite.messaging;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

public class AnsweringLoadTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void severalMessagingsAnsweringLoadTestSingleMorphium(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.getConfig().setMaxConnections(250);
            morphium.getDriver().setMaxConnections(250);
            AtomicInteger messagesSent = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger answersReceived = new AtomicInteger();
            AtomicInteger messagesReceived = new AtomicInteger();
            AtomicLong runtimeTotal = new AtomicLong();
            var amount = 200;
            var recipients = 45;
            var senderThreads = 58;
            var listener = new MessageListener<Msg>() {
                public Msg onMessage(Messaging m, Msg msg) {
                    messagesReceived.incrementAndGet();
                    Msg answer = msg.createAnswerMsg();
                    answer.setMapValue(Doc.of("answer", System.currentTimeMillis()));
                    answer.setPriority(msg.getPriority() - 10);
                    answer.setTimingOut(false);
                    answer.setDeleteAfterProcessing(true);
                    answer.setDeleteAfterProcessingTime(0);
                    return answer;
                }
            };
            var rec = new ArrayList<Messaging>();

            for (int i = 0; i < recipients; i++) {
                Messaging m = new Messaging(morphium);
                m.setSenderId("Rec" + i);
                m.setWindowSize(220);
                m.setPause(100);
                m.setMultithreadded(true);
                m.addListenerForMessageNamed("test", listener);
                m.start();
                rec.add(m);
            }

            log.info("Waiting for messagings to initialise...");
            Thread.sleep(2000); //waiting for messagings to initialize
            var threads = new ArrayList<Thread>();
            var sender = new Messaging(morphium);
            sender.setWindowSize(100);
            sender.setMultithreadded(true);
            sender.setPause(50);
            sender.start();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            } //waiting for init to be complete

            for (int t = 0; t < senderThreads; t++) {
                Thread thr = new Thread(() -> {
                    //warm-up
                    for (int i = 0; i < 2; i++) {
                        Msg m = new Msg("test", "msg", "value", 40000, true);
                        sender.sendAndAwaitFirstAnswer(m, 10000, false);
                    }


                    for (int i = 0; i < amount; i++) {
                        Msg m = new Msg("test", "msg", "value", 40000, true);
                        m.setTimingOut(false);
                        m.setDeleteAfterProcessing(true);
                        m.setDeleteAfterProcessingTime(0);
                        long start = System.currentTimeMillis();
                        var a = sender.sendAndAwaitFirstAnswer(m, 20000, false);
                        long dur = System.currentTimeMillis() - start;

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
                            double averageAnswerTime = ((double) runtimeTotal.get()) / ((double) answersReceived.get());
                            log.info("---> current average: " + averageAnswerTime);
                        }
                    }
                    sender.terminate();

                });
                thr.start();
                threads.add(thr);
            }

            long start = System.currentTimeMillis();

            for (Thread t : threads) {
                t.join();
            }

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
}
