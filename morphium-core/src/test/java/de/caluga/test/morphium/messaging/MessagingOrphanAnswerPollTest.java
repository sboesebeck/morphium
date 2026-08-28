package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Orphaned answers and the poll window (issue #348).
 *
 * An answer nobody awaits - fire-and-forget request, responder answers anyway - is dropped by
 * processing WITHOUT a processed_by mark, on purpose: a listener registered later should still
 * receive it. But the poll's relevance clause admitted EVERY answer (inAnswerTo != null), so such
 * an answer was re-fetched and re-queued on every single tick until its TTL expired. On the genios
 * acceptance cluster that meant ~120 PRIMARY re-fetches per orphan per instance, and - answers
 * carry priority-10 and therefore sort ahead of regular messages - a slot in the poll's
 * limit(windowSize) window for a full minute.
 *
 * The poll only needs answers this instance can actually consume: the ones it is waiting for.
 */
@Tag("core")
public class MessagingOrphanAnswerPollTest {

    private final List<Morphium> morphiums = new ArrayList<>();
    private final List<SingleCollectionMessaging> messagings = new ArrayList<>();

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
     * NOTE: useChangeStream lives on the messaging instance, NOT in MessagingSettings - setting it
     * on the config has no effect. Turning it off here makes the poll the only delivery path, which
     * is what these tests are about.
     */
    private SingleCollectionMessaging start(Morphium m, boolean useChangeStream) {
        SingleCollectionMessaging msg = new SingleCollectionMessaging();
        msg.init(m, m.getConfig().messagingSettings());
        msg.setUseChangeStream(useChangeStream);
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
     * The #348 core case: nobody awaits the answer, so the poll must leave it alone instead of
     * re-queueing it on every tick for its whole TTL.
     */
    @Test
    public void orphanAnswerIsNotRePolledOnEveryTick() throws Exception {
        String db = "orphan_answer_not_repolled";
        Morphium requesterM = newMorphium(db);
        SingleCollectionMessaging requester = start(requesterM, true);
        SingleCollectionMessaging responder = start(newMorphium(db), true);

        // the requester needs some listener, otherwise the poll takes its answers-only branch
        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        responder.addListenerForTopic("orphan_topic", (m, msg) -> msg.createAnswerMsg());

        // fire and forget - no sendAndAwait*, so no waiter and no callback is ever registered
        Msg request = new Msg("orphan_topic", "fire and forget", "value");
        request.setExclusive(true);
        requester.sendMessage(request);

        // let a good number of poll ticks pass (300ms interval)
        Thread.sleep(4000);

        Msg orphan = requesterM.createQueryFor(Msg.class, requester.getCollectionName())
                .f(Msg.Fields.inAnswerTo).ne(null).get();
        assertThat(orphan).as("precondition: the responder's answer is in the collection").isNotNull();
        assertThat(orphan.getProcessedBy() == null || orphan.getProcessedBy().isEmpty())
                .as("precondition: nobody marks an answer no one awaits").isTrue();

        long polled = requester.getProcessingDecisions(orphan.getMsgId()).stream()
                .filter(d -> d.contains("poll: queued for processing"))
                .filter(d -> d.contains(String.valueOf(orphan.getMsgId())))
                .count();

        assertThat(polled)
                .as("an answer nobody awaits must not be picked up by the poll at all")
                .isZero();
    }

    /**
     * Guard against over-filtering: an answer that IS awaited must still be delivered by the poll
     * alone. Change streams are off here, so the poll is the only delivery path.
     */
    @Test
    public void awaitedAnswerIsStillDeliveredByThePoll() throws Exception {
        String db = "awaited_answer_via_poll";
        SingleCollectionMessaging requester = start(newMorphium(db), false);
        SingleCollectionMessaging responder = start(newMorphium(db), false);

        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        responder.addListenerForTopic("awaited_topic", (m, msg) -> msg.createAnswerMsg());

        Msg request = new Msg("awaited_topic", "please answer", "value");
        request.setExclusive(true);
        Msg answer = requester.sendAndAwaitFirstAnswer(request, 8000);

        assertThat(answer)
                .as("an awaited answer must still pass the poll's relevance filter")
                .isNotNull();
    }

    /**
     * Answers can also be awaited by callback rather than by queue (sendAndAwaitAsync stores them
     * in waitingForCallbacks, not waitingForAnswers). Narrowing the poll to awaited answers must
     * cover both, or async requesters lose their poll fallback.
     *
     * Note this passes even before the narrowing, because installStatusInfoListener() keeps
     * listenerByName non-empty, so the poll takes its main branch. It guards the fix, it does not
     * reproduce a bug.
     */
    @Test
    public void answerAwaitedByCallbackIsDeliveredWithoutAnyListener() throws Exception {
        String db = "callback_answer_via_poll";
        SingleCollectionMessaging requester = start(newMorphium(db), false);
        SingleCollectionMessaging responder = start(newMorphium(db), false);

        // deliberately NO listener registered here - but the status info listener keeps
        // listenerByName non-empty, so this still runs through the MAIN branch, not the
        // answers-only one (see the javadoc above). MessagingAnswersOnlyPollBranchTest
        // covers that branch.
        responder.addListenerForTopic("callback_topic", (m, msg) -> msg.createAnswerMsg());

        CountDownLatch got = new CountDownLatch(1);
        AtomicReference<Msg> received = new AtomicReference<>();
        Msg request = new Msg("callback_topic", "answer me via callback", "value");
        request.setExclusive(true);
        requester.sendAndAwaitAsync(request, 8000, m -> {
            received.set(m);
            got.countDown();
        });

        assertThat(got.await(8, TimeUnit.SECONDS))
                .as("the poll must fetch answers awaited by a callback, not just those awaited by a queue")
                .isTrue();
        assertThat(received.get()).isNotNull();
    }
}
