package  de.caluga.test.morphium.messaging;

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
            receiver.addListenerForMessageNamed("test", (msg, m)-> {
                latch.countDown();
                return null;
            });
            receiver.start();
            MorphiumMessaging receiver2 = morph.createMessaging();
            receiver2.setSenderId("rec2");
            receiver2.addListenerForMessageNamed("test", (msg, m)-> {
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
            receiver.addListenerForMessageNamed("test", (msg, m)-> {
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
    public void compareAnsweringBroadcastTest() throws Exception {

        Map<String, Long> runtimes = new HashMap<>();
        int amount = 1000;
        for (String msgImplementation : MorphiumTestBase.messagingsToTest) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);

            final var latch = new java.util.concurrent.CountDownLatch(amount * 2);

            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();
            MorphiumMessaging receiver1 = morph.createMessaging();
            receiver1.setSenderId("rec1");
            receiver1.addListenerForMessageNamed("test", (msg, m)-> {
                latch.countDown();
                return m.createAnswerMsg().setMsg("Recieved by rec1");
            });
            receiver1.start();
            MorphiumMessaging receiver2 = morph.createMessaging();
            receiver2.setSenderId("rec2");
            receiver2.addListenerForMessageNamed("test", (msg, m)-> {
                latch.countDown();
                return m.createAnswerMsg().setMsg("Recieved by rec2");
            });
            receiver2.start();

            Thread.sleep(500);

            for (int i = 0; i < amount; i++) {
                sender.sendMessage(new Msg("test", "test-msg", "test-Value", 30_000, false));

            }

        }
    }
}
