package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The answers-only poll branch: an instance with NO registered listeners (status-info listener
 * disabled) that waits for answers via sendAndAwait*.
 *
 * getMessagesForProcessing() short-circuited this configuration through Query.idList(), whose
 * unbounded generic return type smuggled raw MorphiumIds into the List of
 * ProcessingQueueElement (heap pollution). The first non-empty result then blew up
 * findMessages() with a ClassCastException - swallowed by the poll loop's catch(Throwable) -
 * so the poll fallback and the change stream stall watchdog were both silently dead and the
 * awaited answer never arrived.
 *
 * The branch is only reachable with listenerByName empty, i.e. with the status-info listener
 * disabled (installStatusInfoListener() keeps the map non-empty otherwise) - which is why no
 * other test ever hit it (see MessagingOrphanAnswerPollTest's callback test, which documents
 * exactly this gap). Change streams are off here, so the poll is the only delivery path.
 */
@Tag("messaging")
public class MessagingAnswersOnlyPollBranchTest {

    private final List<Morphium> morphiums = new ArrayList<>();
    private final List<MorphiumMessaging> messagings = new ArrayList<>();

    private Morphium newMorphium(String db, boolean statusInfoListener) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.driverSettings().setInMemorySharedDatabases(true);
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        cfg.messagingSettings().setMessagingPollPause(200);
        // with the status-info listener disabled and no application listeners,
        // listenerByName stays empty and the poll takes its answers-only branch
        cfg.messagingSettings().setMessagingStatusInfoListenerEnabled(statusInfoListener);
        Morphium m = new Morphium(cfg);
        morphiums.add(m);
        return m;
    }

    /**
     * NOTE: for SingleCollectionMessaging useChangeStream lives on the messaging instance, NOT
     * in MessagingSettings - so it is set explicitly here for both implementations. Turning it
     * off makes the poll the only delivery path, which is what these tests are about.
     */
    private <T extends MorphiumMessaging> T start(T msg, Morphium m) {
        msg.init(m, m.getConfig().messagingSettings());
        msg.setUseChangeStream(false);
        msg.start();
        messagings.add(msg);
        return msg;
    }

    @AfterEach
    public void teardown() {
        messagings.forEach(m -> {
            try {
                m.terminate();
            } catch (Exception ignored) {
            }
        });
        morphiums.forEach(m -> {
            try {
                m.close();
            } catch (Exception ignored) {
            }
        });
        messagings.clear();
        morphiums.clear();
    }

    /**
     * SingleCollectionMessaging: no listeners at all on the requester, so the poll takes the
     * answers-only branch - and that branch alone must deliver the awaited answer.
     */
    @Test
    public void answersOnlyBranchDeliversAwaitedAnswerSingleCollection() throws Exception {
        String db = "answers_only_branch_scm";
        SingleCollectionMessaging requester = start(new SingleCollectionMessaging(), newMorphium(db, false));
        SingleCollectionMessaging responder = start(new SingleCollectionMessaging(), newMorphium(db, true));

        responder.addListenerForTopic("answers_only_scm", (m, msg) -> msg.createAnswerMsg());

        Msg request = new Msg("answers_only_scm", "please answer", "value");
        request.setExclusive(true);
        Msg answer = requester.sendAndAwaitFirstAnswer(request, 8000);

        assertThat(answer)
                .as("the answers-only poll branch must deliver an awaited answer")
                .isNotNull();
    }

    /**
     * DualChannelMessaging has the same answers-only branch in its MAIN lane. Answers with a
     * recipient are routed to the DM collection and drained by the separate DM poll, so the
     * responder sends a broadcast answer (inAnswerTo set, no recipient) here - that lands in
     * the main collection, where the answers-only branch is the only consumer.
     */
    @Test
    public void answersOnlyBranchDeliversAwaitedAnswerDualChannel() throws Exception {
        String db = "answers_only_branch_dcm";
        DualChannelMessaging requester = start(new DualChannelMessaging(), newMorphium(db, false));
        DualChannelMessaging responder = start(new DualChannelMessaging(), newMorphium(db, true));

        responder.addListenerForTopic("answers_only_dcm", (m, msg) -> {
            Msg answer = new Msg(msg.getTopic(), "the answer", "value");
            answer.setInAnswerTo(msg.getMsgId());
            // deliberately NO recipient: a directed answer would take the DM lane and bypass
            // the branch under test
            m.sendMessage(answer);
            return null;
        });

        Msg request = new Msg("answers_only_dcm", "please answer", "value");
        request.setExclusive(true);
        Msg answer = requester.sendAndAwaitFirstAnswer(request, 8000);

        assertThat(answer)
                .as("the main lane's answers-only poll branch must deliver an awaited broadcast answer")
                .isNotNull();
    }
}
