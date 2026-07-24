package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOT part of the CI-required suite (see {@link #DualChannelMessagingThroughputSanityTest()}
 * javadoc / class-level {@code @Disabled}, mirroring the existing convention in
 * {@link SpeedTests}). Manual, informal request/reply throughput sanity check for
 * {@link DualChannelMessaging} (#265, beta) - not a regression gate, no meaningful CI assertions,
 * just a quick way to eyeball whether the DM lane's second cursor is roughly living up to the
 * measured expectations in docs/howtos/messaging-implementations.md. Run manually by removing/
 * commenting out the {@code @Disabled} annotation.
 */
@Disabled("Manual throughput sanity check only - not a CI regression gate, see class javadoc")
@Tag("messaging")
public class DualChannelMessagingThroughputSanityTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void requestReplyThroughputSanityTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging requester = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging responder = (DualChannelMessaging) m.createMessaging();

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    responder.start();
                    assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                    responder.addListenerForTopic("throughput", (mm, msg) -> new Msg("throughput", "pong", ""));

                    int total = 2000;
                    CountDownLatch done = new CountDownLatch(total);
                    AtomicLong sumRttNanos = new AtomicLong(0);
                    long start = System.nanoTime();

                    for (int i = 0; i < total; i++) {
                        long sentAt = System.nanoTime();
                        Msg req = new Msg("throughput", "ping", "");
                        requester.sendAndAwaitAsync(req, 20000, incoming -> {
                            sumRttNanos.addAndGet(System.nanoTime() - sentAt);
                            done.countDown();
                        });
                    }

                    boolean finished = done.await(60, TimeUnit.SECONDS);
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    double msgPerSec = total * 1000.0 / Math.max(1, elapsedMs);
                    double avgRttMs = (sumRttNanos.get() / Math.max(1, total)) / 1_000_000.0;

                    log.info("DualChannelMessaging throughput sanity ({}): {} req/reply in {}ms ({} msg/s, avg RTT {}ms), finished={}",
                            m.getDriver().getName(), total, elapsedMs, String.format("%.1f", msgPerSec),
                            String.format("%.2f", avgRttMs), finished);

                    assertTrue(finished, "not all requests completed within the generous 60s bound");
                } finally {
                    requester.terminate();
                    responder.terminate();
                }
            }
        }
    }
}
