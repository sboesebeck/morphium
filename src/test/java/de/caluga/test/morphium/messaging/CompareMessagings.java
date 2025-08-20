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
    public void compareMessagingTests() throws Exception{
        Map<String, Long> runtimes = new HashMap<>();
        int amount = 2000;
        for (String msgImplementation : List.of("StandardMessaging", "AdvMessaging")) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImplementation);
            Morphium morph = new Morphium(cfg);


            MorphiumMessaging sender = morph.createMessaging();
            sender.setSenderId("sender");
            sender.start();

            final AtomicInteger recieved = new AtomicInteger();
            MorphiumMessaging receiver = morph.createMessaging();
            receiver.setSenderId("rec");
            receiver.addListenerForMessageNamed("test", (msg, m)-> {
                recieved.incrementAndGet();
                return null;
            });
            receiver.start();

            Thread.sleep(500);
            log.info("Sending {} messages", amount);
            long start = System.currentTimeMillis();
            Thread.ofVirtual().start(()-> {
                for (int i = 0; i < amount; i++) {
                    sender.sendMessage(new Msg("test", "test-msg", "test-Value", 30000, false));
                }
            });
            log.info("Waiting for messages to be received, already got {}", recieved.get());

            boolean output = false;
            while (recieved.get() != amount) {
                if (recieved.get() % 100 == 0 && !output) {
                    log.info("received {}", recieved.get());
                    output = true;
                }
                if (recieved.get() % 100 != 0) {
                    output = false;
                }
                Thread.yield();
            }
            long dur = System.currentTimeMillis() - start;
            log.info("{} messages send and receive took {} ms with {}", amount, dur, msgImplementation);

            runtimes.put(msgImplementation, dur);
            sender.terminate();
            receiver.terminate();
            morph.close();

        }
        OutputHelper.figletOutput(log, "Results");

        for (var e : runtimes.entrySet()) {
            log.info("{} needed {}ms for {} messages", e.getKey(), e.getValue(), amount);
        }
    }
}
