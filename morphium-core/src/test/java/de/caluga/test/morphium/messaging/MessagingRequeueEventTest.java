package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Requeueing a message by clearing its processedBy via a plain DB update produces no insert
 * event - historically such messages were only found by the interval fallback poll (up to
 * messagingFallbackPollInterval latency, and a requeued message must be found before its TTL
 * expires). The change-stream pipelines now additionally match update events whose
 * updateDescription shows processed_by set to an EMPTY array (the requeue signature - normal
 * processing marks use positional keys like processed_by.0 and must NOT trigger) and react
 * with an immediate poll.
 *
 * The fallback interval is set to 300s here, so a timely delivery proves the EVENT path works.
 */
@Tag("core")
public class MessagingRequeueEventTest {

    private Morphium morphium;
    private MorphiumMessaging sender;
    private MorphiumMessaging receiver;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("msg_requeue_event_test");
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        // Make the fallback poll useless for this test: only the update-event path can deliver
        cfg.messagingSettings().setMessagingFallbackPollInterval(300_000);
        morphium = new Morphium(cfg);
    }

    @AfterEach
    public void teardown() {
        for (MorphiumMessaging m : new MorphiumMessaging[] {sender, receiver}) {
            if (m != null) {
                m.terminate();
            }
        }
        if (morphium != null) {
            morphium.close();
        }
    }

    private void runRequeueScenario(MorphiumMessaging snd, MorphiumMessaging rcv, String topic) throws Exception {
        AtomicInteger received = new AtomicInteger();
        rcv.addListenerForTopic(topic, (msg, m) -> {
            received.incrementAndGet();
            return null;
        });
        Thread.sleep(500); // let the subscription settle

        Msg m = new Msg(topic, "requeue me", "v", 60_000);
        m.setExclusive(true);
        m.setProcessedBy(new ArrayList<>(Arrays.asList("someone_else")));
        snd.sendMessage(m);

        Thread.sleep(1500);
        assertThat(received.get()).as("message marked processed by someone else - must not be delivered").isZero();

        // requeue: clear processedBy via a plain DB update - no insert event is generated
        morphium.setInEntity(m, rcv.getCollectionName(topic),
                Map.of(Msg.Fields.processedBy.name(), new ArrayList<String>()));

        long until = System.currentTimeMillis() + 8_000;
        while (received.get() == 0 && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }

        assertThat(received.get())
                .as("requeued message must be delivered via the processed_by-clear update event - "
                        + "the fallback poll (300s) cannot have rescued it")
                .isEqualTo(1);
    }

    @Test
    public void multiCollectionMessagingDeliversRequeuedMessagesViaUpdateEvent() throws Exception {
        sender = new MultiCollectionMessaging();
        sender.init(morphium);
        sender.start();
        receiver = new MultiCollectionMessaging();
        receiver.init(morphium);
        receiver.start();

        runRequeueScenario(sender, receiver, "requeue_multi");
    }

    @Test
    public void singleCollectionMessagingDeliversRequeuedMessagesViaUpdateEvent() throws Exception {
        sender = new SingleCollectionMessaging();
        sender.init(morphium);
        sender.start();
        receiver = new SingleCollectionMessaging();
        receiver.init(morphium);
        receiver.start();

        runRequeueScenario(sender, receiver, "requeue_single");
    }
}
