package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Messages sent without a TTL (plain {@code new Msg()} leaves ttl at 0 with timingOut=true)
 * used to be dead on arrival: the receive path discards any timing-out message older than its
 * TTL, and 0 means "expired immediately". The default TTL is a setting now
 * (messagingDefaultTtl) and is applied on send; the safety-net fallback poll cadence
 * (messagingFallbackPollInterval) is configurable alongside it, so applications using short
 * TTLs can tighten the net accordingly.
 */
@Tag("core")
public class MessagingDefaultTtlTest {

    private Morphium morphium;
    private MorphiumMessaging messaging;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("msg_default_ttl_test");
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        morphium = new Morphium(cfg);
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
    public void settingsDefaultsMatchTheHistoricalValues() {
        MessagingSettings settings = new MessagingSettings();

        assertThat(settings.getMessagingDefaultTtl())
                .as("default TTL keeps the historical 30s")
                .isEqualTo(Msg.DEFAULT_TTL_MS)
                .isEqualTo(30_000L);
        assertThat(settings.getMessagingFallbackPollInterval())
                .as("fallback poll keeps TTL/3 = 10s")
                .isEqualTo(Msg.DEFAULT_TTL_MS / 3)
                .isEqualTo(10_000L);
    }

    @Test
    public void sendAppliesTheConfiguredDefaultTtlToMessagesWithoutOne() throws Exception {
        // A NON-default value: Msg.preStore() falls back to the historical hardcoded 30s
        // anyway, so only a distinct value proves the SETTING is honored on the send path.
        morphium.getConfig().messagingSettings().setMessagingDefaultTtl(7_777);
        messaging = new MultiCollectionMessaging();
        messaging.init(morphium);
        messaging.start();

        Msg m = new Msg();
        m.setTopic("ttl_test_topic");
        m.setMsg("no ttl set");
        assertThat(m.getTtl()).isZero();

        messaging.sendMessage(m);

        assertThat(m.getTtl())
                .as("send must apply the configured messagingDefaultTtl, not the hardcoded preStore fallback")
                .isEqualTo(7_777L);
    }

    @Test
    public void sendKeepsAnExplicitTtl() throws Exception {
        messaging = new MultiCollectionMessaging();
        messaging.init(morphium);
        messaging.start();

        Msg m = new Msg("ttl_test_topic", "explicit", "v", 5_000);
        messaging.sendMessage(m);

        assertThat(m.getTtl()).as("an explicitly chosen TTL must never be overridden").isEqualTo(5_000L);
    }
}
