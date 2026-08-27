package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Watchdog for the main change stream (issue #346).
 *
 * The watchdog restarts the cursor when the stream falls silent while the fallback poll still
 * finds unprocessed messages. "Poll sees something" alone is not evidence that the stream
 * missed anything: a message another participant is slowly working on stays visible to every
 * other instance's poll, and a message arriving after a quiet spell is simply new. The stream
 * only provably fell behind when a message is NEWER than the last event we received (so the
 * stream owed us that event) and has been sitting there longer than the stall threshold (so it
 * had time to deliver it).
 */
@Tag("core")
public class MessagingCsStallWatchdogTest {

    private final List<Morphium> morphiums = new ArrayList<>();
    private final List<SingleCollectionMessaging> messagings = new ArrayList<>();

    private Morphium newMorphium(String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.driverSettings().setInMemorySharedDatabases(true);
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        // 300ms poll -> 600ms stall threshold, so the test does not have to wait 20s
        cfg.messagingSettings().setMessagingFallbackPollInterval(300);
        Morphium m = new Morphium(cfg);
        morphiums.add(m);
        return m;
    }

    private SingleCollectionMessaging start(Morphium m) {
        SingleCollectionMessaging msg = new SingleCollectionMessaging();
        msg.init(m, m.getConfig().messagingSettings());
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
     * The false alarm from #346: an exclusive message that ANOTHER participant locked and is
     * still processing stays visible to this instance's poll (q1 deliberately does not check the
     * lock collection). The stream delivered the insert event correctly, so nothing is stalled -
     * but with the old predicate every poll tick past the threshold restarted the cursor.
     */
    @Test
    public void doesNotRestartWhileAnotherParticipantProcessesSlowly() throws Exception {
        String db = "cs_stall_watchdog_slow_peer";
        SingleCollectionMessaging slow = start(newMorphium(db));
        SingleCollectionMessaging observer = start(newMorphium(db));
        SingleCollectionMessaging sender = start(newMorphium(db));

        AtomicBoolean processing = new AtomicBoolean(false);
        // both register the topic so both change stream filters accept the broadcast
        slow.addListenerForTopic("slow_topic", (m, msg) -> slowListener(processing));
        observer.addListenerForTopic("slow_topic", (m, msg) -> slowListener(processing));

        waitUntilLive(slow, observer);
        long restartsBefore = slow.getCsStallRestarts() + observer.getCsStallRestarts();

        Msg m = new Msg("slow_topic", "please take your time", "value");
        m.setExclusive(true);
        sender.sendMessage(m);

        // wait out the slow processing: several poll ticks and stall thresholds pass while the
        // message sits there unprocessed
        long until = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < until) {
            Thread.sleep(100);
        }

        assertThat(processing.get())
                .as("precondition: one participant actually picked the message up")
                .isTrue();
        assertThat(slow.getCsStallRestarts() + observer.getCsStallRestarts() - restartsBefore)
                .as("the stream delivered the insert event - a peer being slow is not a stall")
                .isZero();
    }

    /**
     * The quiet-deployment false alarm: after a long silence a fresh message arrives and the
     * poll happens to see it before the change stream event lands. The message is brand new, so
     * the stream cannot have fallen behind yet.
     */
    @Test
    public void doesNotRestartForAMessageThatJustArrivedAfterASilence() throws Exception {
        String db = "cs_stall_watchdog_quiet";
        SingleCollectionMessaging receiver = start(newMorphium(db));
        SingleCollectionMessaging sender = start(newMorphium(db));

        receiver.addListenerForTopic("quiet_topic", (m, msg) -> null);
        waitUntilLive(receiver);

        // stay silent well past the stall threshold (600ms)
        Thread.sleep(2000);
        long restartsBefore = receiver.getCsStallRestarts();

        for (int i = 0; i < 5; i++) {
            sender.sendMessage(new Msg("quiet_topic", "after the silence " + i, "value"));
            Thread.sleep(700);
        }

        assertThat(receiver.getCsStallRestarts() - restartsBefore)
                .as("messages arriving after a quiet spell are new - not evidence of a stalled cursor")
                .isZero();
    }

    /**
     * The production alarm storm (genios acc, 2229 alarms/7d): a fire-and-forget request is
     * answered by the responder, but the requester never registered a waiter or callback for
     * the answer. The answer (recipients=[requester], inAnswerTo set) is delivered by the change
     * stream once, then dropped by the requester's processing WITHOUT a processed_by mark ("no
     * listener for topic" path). From then on the requester's poll re-finds it on EVERY tick
     * (q2 + the inAnswerTo relevance clause) until its TTL expires.
     *
     * The stream delivered that answer correctly, so nothing is stalled - the lingering backlog
     * must not be read as evidence that the cursor fell behind.
     */
    @Test
    public void orphanAnswerDoesNotTriggerFalseRestarts() throws Exception {
        String db = "cs_stall_watchdog_orphan_answer";
        SingleCollectionMessaging requester = start(newMorphium(db));
        SingleCollectionMessaging responder = start(newMorphium(db));

        // the requester needs SOME listener — with an empty listener set the poll switches to
        // its answers-only branch and would not see the orphan at all
        requester.addListenerForTopic("unrelated_topic", (m, msg) -> null);
        // the responder answers every request on the topic
        responder.addListenerForTopic("orphan_topic", (m, msg) -> msg.createAnswerMsg());

        waitUntilLive(requester, responder);
        long requesterBefore = requester.getCsStallRestarts();
        long responderBefore = responder.getCsStallRestarts();

        // fire-and-forget: no sendAndAwait*, so nobody ever consumes the answer
        Msg m = new Msg("orphan_topic", "fire and forget", "value");
        m.setExclusive(true);
        requester.sendMessage(m);

        // several stall thresholds (600ms) pass while the orphan answer sits in the collection
        Thread.sleep(4000);

        // the orphan answer is still there, never marked processed by anyone — permanent backlog
        Morphium morphium = morphiums.get(0);
        Msg orphan = morphium.createQueryFor(Msg.class, requester.getCollectionName())
                .f(Msg.Fields.inAnswerTo).ne(null).get();
        assertThat(orphan).as("precondition: the answer was sent and is still in the collection").isNotNull();
        assertThat(orphan.getProcessedBy() == null || orphan.getProcessedBy().isEmpty())
                .as("nobody ever marks the unawaited answer as processed").isTrue();

        assertThat(requester.getCsStallRestarts() - requesterBefore)
                .as("the stream delivered the answer - a lingering unprocessed message is not a stall")
                .isZero();
        assertThat(responder.getCsStallRestarts() - responderBefore)
                .as("the answer is not addressed to the responder - its poll finds no backlog")
                .isZero();
    }

    /** Occupies the processing slot well past the stall threshold. */
    private Msg slowListener(AtomicBoolean processing) {
        processing.set(true);

        try {
            Thread.sleep(2500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return null;
    }

    private void waitUntilLive(SingleCollectionMessaging... msgs) throws Exception {
        long until = System.currentTimeMillis() + 5000;
        for (SingleCollectionMessaging m : msgs) {
            while (!m.changeStreamsLive() && System.currentTimeMillis() < until) {
                Thread.sleep(50);
            }
            assertThat(m.changeStreamsLive()).as("change stream must be up before the test starts").isTrue();
        }
    }
}
