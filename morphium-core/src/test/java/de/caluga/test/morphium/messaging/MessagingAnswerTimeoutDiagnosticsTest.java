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

    /**
     * The first diagnostics round (stage naming) could still mislead: a real flaky showed the
     * answer WAS sent and its change-stream event received, yet delivery silently failed and
     * the answer was TTL-deleted right at diagnosis time ("answer never sent"). The processing
     * pipeline has several silent skip paths - the diagnostics must therefore include a trace
     * of every processing decision made for the request (and its answers), so the next natural
     * failure names the exact bail-out point.
     */
    @Test
    public void timeoutDiagnosticsIncludeProcessingDecisionTrace() throws Exception {
        SingleCollectionMessaging sender = new SingleCollectionMessaging();
        sender.init(morphium);
        SingleCollectionMessaging responder = new SingleCollectionMessaging();
        responder.init(morphium);
        // listener registered BEFORE start so the change-stream pipeline covers the topic;
        // it deliberately returns no answer - the sender's await must time out
        responder.addListenerForTopic("trace_this_topic", (msging, message) -> null);
        sender.start();
        responder.start();

        try {
            Msg msg = new Msg("trace_this_topic", "q", "v", 5000);
            msg.setExclusive(true);
            sender.sendAndAwaitFirstAnswer(msg, 2000, false);

            // the responder consumed the request but produced no answer - its decision trace
            // must show what happened to the message instead of leaving it to guesswork
            java.util.List<String> decisions = responder.getProcessingDecisions(msg.getMsgId());
            org.junit.jupiter.api.Assertions.assertFalse(decisions.isEmpty(),
                "the responder's processing decisions for the request must be traced");
            org.junit.jupiter.api.Assertions.assertTrue(
                decisions.stream().anyMatch(d -> d.contains("handling")),
                "the trace must show the request being handled, got: " + decisions);
        } finally {
            sender.terminate();
            responder.terminate();
        }
    }

    @Test
    public void multiCollectionMessagingTimeoutSurvivesDiagnostics() {
        assertTimeoutStillThrown(new MultiCollectionMessaging());
    }
}
