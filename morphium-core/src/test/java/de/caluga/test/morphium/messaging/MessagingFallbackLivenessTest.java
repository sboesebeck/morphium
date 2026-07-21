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
 * Change-stream liveness: the watch loop stamps a heartbeat on every server reply (empty
 * batches included). The fallback poll uses it to react immediately when a stream falls
 * silent (the regular interval poll always runs - requeued messages produce no stream
 * event). This test verifies the wiring end to end: once the watch loop runs, the topic
 * reports live.
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
                .as("watch loop is running and stamping heartbeats - the topic must report live")
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
                    .as("message + lock stream heartbeats are running - both streams must report live")
                    .isTrue();
        } finally {
            single.terminate();
        }
    }
}
