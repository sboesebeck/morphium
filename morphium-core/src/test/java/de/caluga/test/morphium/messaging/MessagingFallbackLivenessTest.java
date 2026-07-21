package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MultiCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The messaging fallback poll is liveness-gated: a topic whose change streams demonstrably
 * receive server replies (heartbeat stamped on every batch, empty ones included) is NOT
 * polled at all - the poll only runs when a stream cannot vouch for itself. This test
 * verifies the wiring end to end: once the watch loop runs, the topic reports live.
 */
@Tag("core")
public class MessagingFallbackLivenessTest {

    private Morphium morphium;
    private MultiCollectionMessaging messaging;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("msg_fallback_liveness_test");
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        morphium = new Morphium(cfg);
        messaging = new MultiCollectionMessaging();
        messaging.init(morphium);
        messaging.start();
    }

    @AfterEach
    public void teardown() {
        if (messaging != null) {
            messaging.terminate();
        }
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void subscribedTopicBecomesLiveOnceTheWatchLoopRuns() throws Exception {
        messaging.addListenerForTopic("liveness_topic", (msg, m) -> null);

        long until = System.currentTimeMillis() + 5000;
        while (!messaging.topicStreamsLive("liveness_topic") && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }

        assertThat(messaging.topicStreamsLive("liveness_topic"))
                .as("watch loop is running and stamping heartbeats - the topic must report live, "
                        + "suppressing the fallback poll")
                .isTrue();
    }

    @Test
    public void unknownTopicIsNotLive() {
        assertThat(messaging.topicStreamsLive("nobody_listens_here"))
                .as("no monitors - must be treated as not live so callers poll conservatively")
                .isFalse();
    }

    @Test
    public void singleCollectionMessagingReportsLiveStreams() throws Exception {
        var single = new de.caluga.morphium.messaging.SingleCollectionMessaging();
        single.init(morphium);
        single.start();

        try {
            long until = System.currentTimeMillis() + 5000;
            while (!single.changeStreamsLive() && System.currentTimeMillis() < until) {
                Thread.sleep(50);
            }

            assertThat(single.changeStreamsLive())
                    .as("message + lock stream heartbeats are running - fallback poll must be gated off")
                    .isTrue();
        } finally {
            single.terminate();
        }
    }
}
