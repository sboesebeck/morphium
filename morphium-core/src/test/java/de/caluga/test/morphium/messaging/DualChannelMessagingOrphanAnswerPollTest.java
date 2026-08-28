package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.Msg;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Orphaned answers and the poll window for DualChannelMessaging (issue #348).
 *
 * The #348 fix narrowed the poll's answer-relevance clause from "any answer" (inAnswerTo !=
 * null) to "answers this instance actually awaits" (inAnswerTo in waitingForAnswers +
 * waitingForCallbacks) in THREE places in DualChannelMessaging: the DM-lane poll
 * (findDmMessages), the main lane's answers-only branch, and the main lane's relevance filter.
 * MessagingOrphanAnswerPollTest covers the same fix for SingleCollectionMessaging only - and the
 * two implementations historically drift, so DCM gets its own coverage here.
 *
 * DCM routes answers differently from SCM, so the lane under test depends on the answer's shape:
 * - a DIRECTED answer (recipient set, which is what Msg.createAnswerMsg()/sendAnswer produce)
 *   lands in the requester's per-recipient DM collection and is only ever seen by the DM lane
 *   (DM change stream fast path + findDmMessages poll + processDmElement dispatch);
 * - a BROADCAST answer (inAnswerTo set, NO recipient) lands in the shared main collection and
 *   goes through the main lane's poll (getMessagesForProcessing/findMessages), exactly like SCM.
 * See MessagingAnswersOnlyPollBranchTest for the same distinction on the answers-only branch.
 */
@Tag("messaging")
public class DualChannelMessagingOrphanAnswerPollTest {

    private final List<Morphium> morphiums = new ArrayList<>();
    private final List<DualChannelMessaging> messagings = new ArrayList<>();

    private Morphium newMorphium(String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.driverSettings().setInMemorySharedDatabases(true);
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        cfg.messagingSettings().setMessagingFallbackPollInterval(300);
        Morphium m = new Morphium(cfg);
        morphiums.add(m);
        return m;
    }

    /**
     * NOTE: useChangeStream lives on the messaging instance, NOT in MessagingSettings - setting
     * it on the config has no effect. Turning it off makes the polls (main lane AND DM lane) the
     * only delivery paths, which is what the "still delivered" tests below are about.
     */
    private DualChannelMessaging start(Morphium m, boolean useChangeStream) throws Exception {
        DualChannelMessaging msg = new DualChannelMessaging();
        msg.init(m, m.getConfig().messagingSettings());
        msg.setUseChangeStream(useChangeStream);
        msg.start();
        messagings.add(msg);

        if (useChangeStream) {
            assertThat(msg.waitForReady(30, TimeUnit.SECONDS)).isTrue();
        }

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
     * MAIN lane: a broadcast answer (no recipient) nobody awaits lands in the main collection,
     * is dropped by processing WITHOUT a processed_by mark (on purpose - a listener registered
     * later should still get it), and must NOT be re-fetched by the main poll on every tick.
     *
     * Evidence: DCM's main-lane poll shares SCM's decision trace, so "poll: queued for
     * processing" entries for the orphan prove a poll pickup - same technique as
     * MessagingOrphanAnswerPollTest.
     */
    @Test
    public void orphanBroadcastAnswerIsNotRePolledByMainLanePoll() throws Exception {
        String db = "dcm_orphan_bcast_answer";
        Morphium requesterM = newMorphium(db);
        DualChannelMessaging requester = start(requesterM, true);
        DualChannelMessaging responder = start(newMorphium(db), true);

        // the requester needs some listener, otherwise the main poll takes its answers-only branch
        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        responder.addListenerForTopic("dcm_orphan_bcast", (m, msg) -> {
            Msg answer = new Msg(msg.getTopic(), "the answer", "value");
            answer.setInAnswerTo(msg.getMsgId());
            // deliberately NO recipient: a directed answer would take the DM lane and never be
            // seen by the main-lane poll under test here (see class javadoc)
            m.sendMessage(answer);
            return null;
        });

        // fire and forget - no sendAndAwait*, so no waiter and no callback is ever registered
        Msg request = new Msg("dcm_orphan_bcast", "fire and forget", "value");
        request.setExclusive(true);
        requester.sendMessage(request);

        // let a good number of poll ticks pass (300ms fallback interval)
        Thread.sleep(4000);

        Msg orphan = requesterM.createQueryFor(Msg.class, requester.getCollectionName())
                .f(Msg.Fields.inAnswerTo).ne(null).get();
        assertThat(orphan).as("precondition: the broadcast answer is in the main collection").isNotNull();
        assertThat(orphan.getProcessedBy() == null || orphan.getProcessedBy().isEmpty())
                .as("precondition: nobody marks an answer no one awaits").isTrue();

        long polled = requester.getProcessingDecisions(orphan.getMsgId()).stream()
                .filter(d -> d.contains("poll: queued for processing"))
                .filter(d -> d.contains(String.valueOf(orphan.getMsgId())))
                .count();

        assertThat(polled)
                .as("an answer nobody awaits must not be picked up by the main-lane poll at all")
                .isZero();
    }

    /**
     * DM lane: a DIRECTED answer nobody awaits (createAnswerMsg sets the requester as recipient)
     * lands in the requester's DM collection. The DM change stream delivers it once
     * (handleDmAnswer finds neither waiter nor callback and falls through to processDmElement,
     * which drops it without a processed_by mark), and findDmMessages must then leave it alone
     * instead of re-fetching it on every tick until TTL.
     *
     * Evidence: the DM lane has NO decision trace (traceDecision only covers the main lane), so
     * the trace technique of the test above does not work here. Instead this counts entity
     * materializations of the orphan via Morphium's postLoad storage listener: findDmMessages
     * fetches FULL Msg documents via Query.asList() (no projection, unlike the main poll) and
     * processDmElement re-reads via Query.get() - both fire postLoad per entity, while the
     * change stream fast path deserializes via the mapper directly and does not. Every poll
     * re-fetch therefore shows up as at least one postLoad of the orphan.
     */
    @Test
    public void orphanDirectedAnswerIsNotRePolledByDmPoll() throws Exception {
        String db = "dcm_orphan_dm_answer";
        Morphium requesterM = newMorphium(db);

        AtomicInteger orphanLoads = new AtomicInteger(0);
        AtomicReference<MorphiumId> requestId = new AtomicReference<>();
        // NOTE: storage listeners are NOT filtered by entity type (the generic is erased), so
        // this receives postLoad for every entity the requester's Morphium reads - filter by hand
        requesterM.addListener(new MorphiumStorageAdapter<Object>() {
            @Override
            public void postLoad(Morphium m, Object o) {
                if (o instanceof Msg msg && requestId.get() != null
                        && requestId.get().equals(msg.getInAnswerTo())) {
                    orphanLoads.incrementAndGet();
                }
            }
        });

        DualChannelMessaging requester = start(requesterM, true);
        DualChannelMessaging responder = start(newMorphium(db), true);

        // keep the main lane in its regular branch, same as the test above - irrelevant for the
        // DM lane (findDmMessages has no answers-only branching) but avoids mixing concerns
        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        // createAnswerMsg addresses the requester -> the answer takes the DM lane
        responder.addListenerForTopic("dcm_orphan_dm", (m, msg) -> msg.createAnswerMsg());

        Msg request = new Msg("dcm_orphan_dm", "fire and forget", "value");
        request.setExclusive(true);
        requester.sendMessage(request);
        requestId.set(request.getMsgId());

        // let a good number of DM poll ticks pass (300ms fallback interval)
        Thread.sleep(4000);

        Msg orphan = requesterM.createQueryFor(Msg.class, requester.getDMCollectionName())
                .f(Msg.Fields.inAnswerTo).eq(request.getMsgId()).get();
        assertThat(orphan).as("precondition: the directed answer is in the requester's DM collection").isNotNull();
        assertThat(orphan.getProcessedBy() == null || !orphan.getProcessedBy().contains(requester.getSenderId()))
                .as("precondition: nobody marks an answer no one awaits").isTrue();

        // one load comes from the CS fast path's fallthrough dispatch (processDmElement re-read);
        // the precondition query above adds one more. A poll re-fetching the orphan on every
        // tick would push this to roughly one load per 300ms over 4s, i.e. well above 10.
        assertThat(orphanLoads.get())
                .as("an unawaited directed answer must not be re-fetched by the DM poll on every tick")
                .isLessThanOrEqualTo(3);
    }

    /**
     * Guard against over-filtering, DM lane, waiter path: a directed answer that IS awaited via
     * sendAndAwaitFirstAnswer (waitingForAnswers) must still be delivered when the change
     * streams are off and findDmMessages -> processDmElement is the only delivery path.
     */
    @Test
    public void awaitedDirectedAnswerIsDeliveredByDmPollAlone() throws Exception {
        String db = "dcm_awaited_dm_answer";
        DualChannelMessaging requester = start(newMorphium(db), false);
        DualChannelMessaging responder = start(newMorphium(db), false);

        responder.addListenerForTopic("dcm_awaited_dm", (m, msg) -> msg.createAnswerMsg());

        Msg request = new Msg("dcm_awaited_dm", "please answer", "value");
        request.setExclusive(true);
        Msg answer = requester.sendAndAwaitFirstAnswer(request, 8000);

        assertThat(answer)
                .as("an awaited directed answer must still pass the DM poll's relevance filter")
                .isNotNull();
    }

    /**
     * Guard against over-filtering, DM lane, callback path: answers awaited via sendAndAwaitAsync
     * are registered in waitingForCallbacks, NOT waitingForAnswers. The narrowed filter must
     * cover both maps (awaitedAnswerIds), or async requesters lose their DM poll fallback.
     */
    @Test
    public void directedAnswerAwaitedByCallbackIsDeliveredByDmPollAlone() throws Exception {
        String db = "dcm_callback_dm_answer";
        DualChannelMessaging requester = start(newMorphium(db), false);
        DualChannelMessaging responder = start(newMorphium(db), false);

        responder.addListenerForTopic("dcm_callback_dm", (m, msg) -> msg.createAnswerMsg());

        CountDownLatch got = new CountDownLatch(1);
        AtomicReference<Msg> received = new AtomicReference<>();
        Msg request = new Msg("dcm_callback_dm", "answer me via callback", "value");
        request.setExclusive(true);
        requester.sendAndAwaitAsync(request, 8000, m -> {
            received.set(m);
            got.countDown();
        });

        assertThat(got.await(8, TimeUnit.SECONDS))
                .as("the DM poll must fetch answers awaited by a callback, not just those awaited by a queue")
                .isTrue();
        assertThat(received.get()).isNotNull();
    }

    /**
     * Guard against over-filtering, MAIN lane relevance clause: a broadcast answer awaited by a
     * waiter must still be delivered by the main poll alone. The requester has a topic listener
     * registered, so the poll takes its MAIN branch (with the relevance or-clause) and not the
     * answers-only branch - the latter is covered by MessagingAnswersOnlyPollBranchTest.
     */
    @Test
    public void awaitedBroadcastAnswerIsDeliveredByMainLanePollAlone() throws Exception {
        String db = "dcm_awaited_bcast_answer";
        DualChannelMessaging requester = start(newMorphium(db), false);
        DualChannelMessaging responder = start(newMorphium(db), false);

        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        responder.addListenerForTopic("dcm_awaited_bcast", (m, msg) -> {
            Msg answer = new Msg(msg.getTopic(), "the answer", "value");
            answer.setInAnswerTo(msg.getMsgId());
            // NO recipient: keep the answer in the main collection / main lane (see class javadoc)
            m.sendMessage(answer);
            return null;
        });

        Msg request = new Msg("dcm_awaited_bcast", "please answer", "value");
        request.setExclusive(true);
        Msg answer = requester.sendAndAwaitFirstAnswer(request, 8000);

        assertThat(answer)
                .as("an awaited broadcast answer must still pass the main-lane poll's relevance filter")
                .isNotNull();
    }

    /**
     * Same as above for the callback registration (waitingForCallbacks) - the main lane's
     * relevance clause must admit callback-awaited answers too.
     */
    @Test
    public void broadcastAnswerAwaitedByCallbackIsDeliveredByMainLanePollAlone() throws Exception {
        String db = "dcm_callback_bcast_answer";
        DualChannelMessaging requester = start(newMorphium(db), false);
        DualChannelMessaging responder = start(newMorphium(db), false);

        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        responder.addListenerForTopic("dcm_callback_bcast", (m, msg) -> {
            Msg answer = new Msg(msg.getTopic(), "the answer", "value");
            answer.setInAnswerTo(msg.getMsgId());
            m.sendMessage(answer);
            return null;
        });

        CountDownLatch got = new CountDownLatch(1);
        AtomicReference<Msg> received = new AtomicReference<>();
        Msg request = new Msg("dcm_callback_bcast", "answer me via callback", "value");
        request.setExclusive(true);
        requester.sendAndAwaitAsync(request, 8000, m -> {
            received.set(m);
            got.countDown();
        });

        assertThat(got.await(8, TimeUnit.SECONDS))
                .as("the main-lane poll must fetch answers awaited by a callback as well")
                .isTrue();
        assertThat(received.get()).isNotNull();
    }
}
