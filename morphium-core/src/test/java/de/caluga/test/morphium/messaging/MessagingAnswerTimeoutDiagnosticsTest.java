package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging.MessageTimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The answer-timeout path carries failure diagnostics now (which stage failed: request
 * delivery/processing, answer send, or answer delivery) - see the recurring BasicJMSTests
 * flaky. This locks the contract: on timeout the diagnostics run WITHOUT masking the
 * MessageTimeoutException, for both messaging implementations.
 */
@Tag("core")
public class MessagingAnswerTimeoutDiagnosticsTest {

    private Morphium morphium;
    private MorphiumMessaging messaging;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("msg_timeout_diag_test");
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

    private void assertTimeoutStillThrown(MorphiumMessaging m) {
        m.init(morphium);
        m.start();
        messaging = m;

        // nobody answers - the timeout must survive the diagnostics pass unchanged
        assertThatThrownBy(() -> m.sendAndAwaitFirstAnswer(
                new Msg("nobody_answers_here", "q", "v", 5000), 300, true))
            .isInstanceOf(MessageTimeoutException.class)
            .hasMessageContaining("Did not receive answer");
    }

    @Test
    public void singleCollectionMessagingTimeoutSurvivesDiagnostics() {
        assertTimeoutStillThrown(new SingleCollectionMessaging());
    }

    @Test
    public void multiCollectionMessagingTimeoutSurvivesDiagnostics() {
        assertTimeoutStillThrown(new MultiCollectionMessaging());
    }
}
