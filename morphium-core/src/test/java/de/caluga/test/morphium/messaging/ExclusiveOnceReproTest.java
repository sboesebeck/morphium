package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

/**
 * Repro for prod duplicate processing of exclusive messages (JEF: 1 msg_id -> 2 tasks -> 2 invoices).
 *
 * Scenario: exclusive message, listener does NOT set markAsProcessedBeforeExec (default) -> processed_by
 * stays empty during onMessage, exactly-once relies solely on the MsgLock. If the lock is lost
 * mid-processing (TTL, cleanup, failover) AND the message is picked up via the poll path (active on
 * change-stream stall/watchdog), a second instance re-locks, sees empty processed_by and processes again.
 */
@Tag("messaging")
// pooled-only repro (lock-loss needs a real server); "external" keeps it out of inmem-phase
// runs where getMorphiumInstancesPooledOnly() yields no arguments and JUnit errors out
@Tag("external")
public class ExclusiveOnceReproTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void exclusiveDoubleProcessedWhenLockLostMidProcessing(Morphium morphium) throws Exception {
        try (morphium) {
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(SingleCollectionMessaging.NAME);
            // poll path only - mirrors prod condition (change stream stalled / watchdog restart)
            cfg.messagingSettings().setUseChangeStream(false);
            cfg.encryptionSettings()
               .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                Thread.sleep(500);

                AtomicInteger processings = new AtomicInteger();
                CountDownLatch processingStarted = new CountDownLatch(1);
                MessageListener<Msg> slowListener = (msging, msg) -> {
                    processings.incrementAndGet();
                    processingStarted.countDown();

                    try {
                        // long running job (mirrors InvoiceDoerJob) - window for lock loss
                        Thread.sleep(4000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                };

                MorphiumMessaging a = m.createMessaging();
                a.setSenderId("procA");
                a.setPause(300).setMultithreadded(true).setWindowSize(10);
                a.addListenerForTopic("repro", slowListener);
                MorphiumMessaging b = m.createMessaging();
                b.setSenderId("procB");
                b.setPause(300).setMultithreadded(true).setWindowSize(10);
                b.addListenerForTopic("repro", slowListener);
                MorphiumMessaging sender = m.createMessaging();
                sender.setSenderId("sender");

                try {
                    a.start();
                    assertTrue(a.waitForReady(30, TimeUnit.SECONDS), "a not ready");
                    b.start();
                    assertTrue(b.waitForReady(30, TimeUnit.SECONDS), "b not ready");
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");

                    Msg msg = new Msg("repro", "do the thing", "value", 120000, true);
                    sender.sendMessage(msg);

                    assertTrue(processingStarted.await(30, TimeUnit.SECONDS), "no listener started processing");
                    // make sure we are well inside onMessage
                    Thread.sleep(500);

                    // fault injection: lock disappears mid-processing
                    var deleted = m.createQueryFor(MsgLock.class, a.getLockCollectionName())
                                    .f("_id").eq(msg.getMsgId()).delete();
                    log.info("deleted locks: {}", deleted);

                    // give the poll path plenty of cycles to (wrongly) re-deliver
                    Thread.sleep(10000);

                    assertEquals(1, processings.get(),
                                 "exclusive message was processed more than once after lock loss");
                } finally {
                    sender.terminate();
                    a.terminate();
                    b.terminate();
                }
            }
        }
    }
}
