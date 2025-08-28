package  de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class CompareMessagings extends MorphiumTestBase {

    @Test
    public void compareExclMessagingTests() throws Exception{
        Map<String, Long> runtimes = new HashMap<>();
        int amount = 500;
        for (String msgImplementation : MorphiumTestBase.messagingsToTest) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);


            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();

            final var latch = new java.util.concurrent.CountDownLatch(amount);
            MorphiumMessaging receiver = morph.createMessaging();
            receiver.setSenderId("rec");
            receiver.addListenerForTopic("test", (msg, m)-> {
                latch.countDown();
                return null;
            });
            receiver.start();
            MorphiumMessaging receiver2 = morph.createMessaging();
            receiver2.setSenderId("rec2");
            receiver2.addListenerForTopic("test", (msg, m)-> {
                latch.countDown();
                return null;
            });
            receiver2.start();

            Thread.sleep(500);

            var received = new java.util.concurrent.atomic.AtomicInteger(0);
            var scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                int done = (int) (amount - latch.getCount());
                if (done != received.getAndSet(done)) {
                    log.info("received {}", done);
                }
            }, 500, 500, java.util.concurrent.TimeUnit.MILLISECONDS);

            long start = System.nanoTime();

            Thread sd = Thread.ofVirtual().start(() -> {
                for (int i = 0; i < amount; i++) {
                    try {
                        sender.sendMessage(new Msg("test", "test-msg", "test-Value", 30_000, true));
                    } catch (Exception e) {
                        log.warn("send failed at {}: {}", i, e.toString());
                    }
                }
            });

            // Warten bis alle empfangen sind – mit Timeout
            boolean allReceived = latch.await(90, java.util.concurrent.TimeUnit.SECONDS);
            long durMs = (System.nanoTime() - start) / 1_000_000;

            scheduler.shutdownNow();
            sd.join(); // sauber beenden

            if (!allReceived) {
                long got = amount - latch.getCount();
                log.error("Timeout: received {}/{} after {} ms", got, amount, durMs);
            } else {
                log.info("{} messages send & receive took {} ms ({}/s)",
                         amount, durMs, durMs == 0 ? "∞" : (amount * 1000L) / durMs);
            }



            runtimes.put(msgImplementation, durMs);

            try {
                sender.terminate();
            } catch (Exception e) {
                log.error("could not terminate sender", e);
            }
            try {
                receiver.terminate();
            } catch (Exception e) {
                log.error("could not terminate rec1", e);
            }
            try {
                receiver2.terminate();
            } catch (Exception e) {
                log.error("could not terminate rec2", e);
            }

            morph.close();

        }
        OutputHelper.figletOutput(log, "Results");

        for (var e : runtimes.entrySet()) {
            log.info("{} needed {}ms for {} messages", e.getKey(), e.getValue(), amount);
        }
    }
    @Test
    public void compareMessagingTests() throws Exception{
        Map<String, Long> runtimes = new HashMap<>();
        int amount = 1000;
        for (String msgImplementation : MorphiumTestBase.messagingsToTest) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);


            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();

            final var latch = new java.util.concurrent.CountDownLatch(amount);
            final AtomicInteger recieved = new AtomicInteger();
            MorphiumMessaging receiver = morph.createMessaging();
            receiver.setSenderId("rec");
            receiver.addListenerForTopic("test", (msg, m)-> {
                latch.countDown();
                return null;
            });
            receiver.start();

            Thread.sleep(500);

            var received = new java.util.concurrent.atomic.AtomicInteger(0);
            var scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                int done = (int) (amount - latch.getCount());
                if (done != received.getAndSet(done)) {
                    log.info("received {}", done);
                }
            }, 500, 500, java.util.concurrent.TimeUnit.MILLISECONDS);

            long start = System.nanoTime();

            Thread sd = Thread.ofVirtual().start(() -> {
                for (int i = 0; i < amount; i++) {
                    try {
                        sender.sendMessage(new Msg("test", "test-msg", "test-Value", 30_000, false));
                    } catch (Exception e) {
                        log.warn("send failed at {}: {}", i, e.toString());
                    }
                }
            });

            // Warten bis alle empfangen sind – mit Timeout
            boolean allReceived = latch.await(90, java.util.concurrent.TimeUnit.SECONDS);
            long durMs = (System.nanoTime() - start) / 1_000_000;

            scheduler.shutdownNow();
            sd.join(); // sauber beenden

            if (!allReceived) {
                long got = amount - latch.getCount();
                log.error("Timeout: received {}/{} after {} ms", got, amount, durMs);
            } else {
                log.info("{} messages send & receive took {} ms ({}/s)",
                         amount, durMs, durMs == 0 ? "∞" : (amount * 1000L) / durMs);
            }



            runtimes.put(msgImplementation, durMs);
            try {
                sender.terminate();
            } catch (Exception e) {
                log.error("Could not erminate sender", e);
            }
            try {
                receiver.terminate();
            } catch (Exception e) {
                log.error("Could not erminate receiver", e);
            }
            morph.close();

        }
        OutputHelper.figletOutput(log, "Results");

        for (var e : runtimes.entrySet()) {
            log.info("{} needed {}ms for {} messages", e.getKey(), e.getValue(), amount);
        }
    }




    @Test
    public void compareAnsweringExclusiveTest() throws Exception {

        Map<String, Long> runtimes = new HashMap<>();
        int amount = 200;
        int receiverAmount = 3;
        for (String msgImplementation : MorphiumTestBase.messagingsToTest) {
            List<MorphiumMessaging> receivers = new ArrayList<>();
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);

            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();

            for (int i = 0; i < receiverAmount; i++) {
                MorphiumMessaging receiver1 = morph.createMessaging();
                receiver1.setSenderId("rec" + i);
                receiver1.addListenerForTopic("test", (msg, m)-> {
                    return m.createAnswerMsg().setMsg("Recieved by rec");
                });
                receiver1.start();
                receivers.add(receiver1);
            }

            Thread.sleep(500);
            //warmup
            var answers = sender.sendAndAwaitAnswers(new Msg("test", "test-msg", "test-Value", 30_000, true), 1, 10000, false);
            assertEquals(1, answers.size(), "Wrong amount of answers?");

            long total = 0;
            for (int i = 0; i < amount; i++) {
                long start = System.currentTimeMillis();
                // answers = sender.sendAndAwaitAnswers(new Msg("test", "test-msg", "test-Value", 30_000, true), 1, 10000, false);
                var a = sender.sendAndAwaitFirstAnswer(new Msg("test", "test-msg", "test-value", 30000, true), 10000, false);
                long dur = System.currentTimeMillis() - start;
                log.info("{}: Getting answer took {}ms", i, dur);
                total = total + dur;
                assertNotNull(a);
            }
            runtimes.put(msgImplementation, total);
            log.info("Processed all {} queries for {}", amount, msgImplementation);

            sender.terminate();
            for (MorphiumMessaging r : receivers) {
                r.terminate();
            }
            receivers.clear();

        }
        OutputHelper.figletOutput(log, "Done!");
        for (String impl : runtimes.keySet()) {
            log.info("Implementation {} needed {}ms", impl, runtimes.get(impl));
        }
    }
    @Test
    public void compareAnsweringBroadcastTest() throws Exception {

        Map<String, Long> runtimes = new HashMap<>();
        int amount = 200;
        int receiverAmount = 3;
        for (String msgImplementation : MorphiumTestBase.messagingsToTest) {
            List<MorphiumMessaging> receivers = new ArrayList<>();
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);

            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();

            for (int i = 0; i < receiverAmount; i++) {
                MorphiumMessaging receiver1 = morph.createMessaging();
                receiver1.setSenderId("rec" + i);
                receiver1.addListenerForTopic("test", (msg, m)-> {
                    return m.createAnswerMsg().setMsg("Recieved by rec");
                });
                receiver1.start();
                receivers.add(receiver1);
            }

            Thread.sleep(500);
            //warmup
            var answers = sender.sendAndAwaitAnswers(new Msg("test", "test-msg", "test-Value", 30_000, false), receiverAmount, 10000, false);
            assertEquals(receiverAmount, answers.size(), "Wrong amount of answers?");

            long total = 0;
            for (int i = 0; i < amount; i++) {
                long start = System.currentTimeMillis();
                answers = sender.sendAndAwaitAnswers(new Msg("test", "test-msg", "test-Value", 30_000, false), receiverAmount, 10000, false);
                long dur = System.currentTimeMillis() - start;
                log.info("{}: Getting answers took {}ms", i, dur);
                total = total + dur;
                assertNotNull(answers);
                assertFalse(answers.isEmpty());
                assertEquals(receiverAmount, answers.size(), "wrong amount of answers" );
            }
            runtimes.put(msgImplementation, total);
            log.info("Processed all {} queries for {}", amount, msgImplementation);

            sender.terminate();
            for (MorphiumMessaging r : receivers) {
                r.terminate();
            }
            receivers.clear();

        }
        OutputHelper.figletOutput(log, "Done!");
        for (String impl : runtimes.keySet()) {
            log.info("Implementation {} needed {}ms", impl, runtimes.get(impl));
        }
    }
}
