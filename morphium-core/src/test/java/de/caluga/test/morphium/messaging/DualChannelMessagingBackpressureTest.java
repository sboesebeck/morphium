package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Backpressure coverage for {@link DualChannelMessaging} (#265, beta): a burst of concurrent
 * {@code sendAndAwaitAsync} requests against an artificially slow responder must keep the DM
 * lane's internal queue bounded (overflow guard, see {@code enqueueDmForProcessing}) rather than
 * growing unbounded, and every request must either complete or cleanly time out - none may hang
 * forever. Sized to stay CI-friendly (well under a minute).
 */
@Tag("messaging")
public class DualChannelMessagingBackpressureTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void burstOfRequestsKeepsDmQueueBoundedTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
            cfg.messagingSettings().setMessagingWindowSize(20);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging requester = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging responder = (DualChannelMessaging) m.createMessaging();

                int windowSize = requester.getWindowSize();
                int burst = 300;
                AtomicInteger completedCallbacks = new AtomicInteger(0);
                AtomicInteger maxObservedQueueSize = new AtomicInteger(0);
                CountDownLatch allDone = new CountDownLatch(burst);

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    responder.start();
                    assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                    responder.addListenerForTopic("slow", (mm, msg) -> {
                        try {
                            Thread.sleep(50); // artificially slow responder
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                        return new Msg("slow", "done", "");
                    });

                    // Monitor both instances' DM dispatcher backlog while the burst is in flight -
                    // the overflow guard in enqueueDmForProcessing caps dmProcessing at 2x windowSize.
                    AtomicInteger maxRequesterDmBacklog = new AtomicInteger(0);
                    AtomicInteger maxResponderDmBacklog = new AtomicInteger(0);
                    Thread monitor = Thread.ofPlatform().start(() -> {
                        while (allDone.getCount() > 0) {
                            maxRequesterDmBacklog.set(Math.max(maxRequesterDmBacklog.get(), requester.getDmProcessingCount()));
                            maxResponderDmBacklog.set(Math.max(maxResponderDmBacklog.get(), responder.getDmProcessingCount()));
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    });

                    long start = System.currentTimeMillis();

                    for (int i = 0; i < burst; i++) {
                        Msg req = new Msg("slow", "ping", "");
                        requester.sendAndAwaitAsync(req, 45000, incoming -> {
                            completedCallbacks.incrementAndGet();
                            allDone.countDown();
                        });
                    }

                    boolean finished = allDone.await(55, TimeUnit.SECONDS);
                    monitor.interrupt();
                    long elapsed = System.currentTimeMillis() - start;

                    assertTrue(finished, "not all " + burst + " requests completed or timed out within 55s (completed="
                            + completedCallbacks.get() + "/" + burst + ")");
                    assertEquals(burst, completedCallbacks.get(), "every request must get a callback (answer or timeout)");
                    log.info("Backpressure burst of {} requests completed in {}ms (windowSize={}, maxRequesterDmBacklog={}, maxResponderDmBacklog={})",
                            burst, elapsed, windowSize, maxRequesterDmBacklog.get(), maxResponderDmBacklog.get());

                    int bound = 2 * windowSize;
                    assertTrue(maxRequesterDmBacklog.get() <= bound,
                            "requester DM queue exceeded overflow guard: " + maxRequesterDmBacklog.get() + " > " + bound);
                    assertTrue(maxResponderDmBacklog.get() <= bound,
                            "responder DM queue exceeded overflow guard: " + maxResponderDmBacklog.get() + " > " + bound);
                } finally {
                    requester.terminate();
                    responder.terminate();
                }
            }
        }
    }
}
