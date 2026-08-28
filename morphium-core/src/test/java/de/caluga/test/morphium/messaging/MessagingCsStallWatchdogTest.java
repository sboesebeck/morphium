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
     * listener for topic" path).
     *
     * The stream delivered that answer correctly, so nothing is stalled - the lingering backlog
     * must not be read as evidence that the cursor fell behind.
     *
     * HISTORY - when this test was written, the poll's relevance clause admitted every answer
     * (inAnswerTo != null), so the orphan was re-found on EVERY tick and drove the alarm storm.
     * #348 has since narrowed that clause to awaited answers only, so the requester's poll no
     * longer sees the orphan at all and this passes for a second, weaker reason. It is kept as a
     * regression guard for the production scenario end to end; the discriminating test for the
     * watchdog predicate itself is doesNotRestartWhileAnotherParticipantProcessesSlowly above.
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

    /**
     * The positive case: a cursor that SILENTLY stops delivering while traffic keeps flowing
     * must trip the watchdog and get restarted. Without this test, a "return;" as the first
     * line of restartMainCsIfStalled() would go unnoticed - the three tests above only prove
     * the absence of false alarms.
     *
     * Failure injection: poll filter and change stream filter are deliberately congruent, so a
     * healthy stream never leaves an unannounced message behind - a real stall cannot be
     * provoked through the public API. Instead of a production-code test seam, this reaches
     * into the RECEIVER's live ChangeStreamMonitor via reflection and clears its listener list:
     * the watch loop keeps running and isStreamLive() stays true (so the messaging does not
     * notice anything and keeps polling on the regular fallback timer), but no event reaches
     * onMainCsEvent() anymore - exactly the "cursor delivers nothing, nobody knows" profile
     * the watchdog exists for.
     *
     * TIMING - the watchdog is traffic-gated (see the KNOWN LIMIT comment in
     * restartMainCsIfStalled): a polled message is consumed within a tick or two and is then
     * excluded from the next poll (processed_by / idsToIgnore), so it is evidence only briefly,
     * while the unannounced-marker must HOLD for a full threshold (600ms here). The sender below
     * therefore provides a continuous stream: one fresh message per poll interval (150ms sends
     * vs 300ms interval) is guaranteed to keep the marker armed. Measured with this setup:
     * arrival intervals up to 700ms still trip the alarm (a message stays poll-visible slightly
     * past the tick that found it, because the processed_by mark lands just after the next tick
     * starts), intervals of 1000ms and above never do - the marker resets in the gaps and a dead
     * cursor on such quiet traffic degrades silently to poll latency, exactly as the KNOWN LIMIT
     * comment predicts.
     */
    @Test
    public void restartsWhenTheStreamGoesSilentUnderContinuousTraffic() throws Exception {
        String db = "cs_stall_watchdog_positive";
        SingleCollectionMessaging receiver = start(newMorphium(db));
        SingleCollectionMessaging sender = start(newMorphium(db));

        receiver.addListenerForTopic("stall_topic", (m, msg) -> null);
        waitUntilLive(receiver);
        // registering the listener makes the poll loop rebuild the CS filter (and with it the
        // monitor) on one of the next ticks - wait that out, or the rebuild would replace the
        // monitor we are about to silence and undo the injection
        long until = System.currentTimeMillis() + 5000;

        while (!receiver.getCsFilterTopics().contains("stall_topic") && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }

        assertThat(receiver.getCsFilterTopics()).as("CS filter rebuild must have settled before injecting").contains("stall_topic");

        Object silencedMonitor = silenceMainCs(receiver);
        long restartsBefore = receiver.getCsStallRestarts();

        // continuous traffic while the cursor is dead: one fresh message per 150ms, i.e. at
        // least one per 300ms poll tick - the minimum rate for the hold-gate (see javadoc)
        long deadline = System.currentTimeMillis() + 8000;
        int sent = 0;

        while (System.currentTimeMillis() < deadline && receiver.getCsStallRestarts() == restartsBefore) {
            Msg m = new Msg("stall_topic", "keepalive " + (sent++), "value");
            m.setExclusive(false);
            sender.sendMessage(m);
            Thread.sleep(150);
        }

        assertThat(receiver.getCsStallRestarts() - restartsBefore)
                .as("a stream that stops announcing while fresh messages keep arriving is a stall - "
                    + "the watchdog must restart the cursor")
                .isGreaterThanOrEqualTo(1);
        // and the restart must actually have replaced the silenced cursor with a live one
        assertThat(mainCsMonitorOf(receiver))
                .as("the restart must install a FRESH monitor, not re-trigger the dead one")
                .isNotSameAs(silencedMonitor);
        assertThat(receiver.changeStreamsLive())
                .as("after the restart the main stream must be live again")
                .isTrue();
    }

    /**
     * Failure injection (see restartsWhenTheStreamGoesSilentUnderContinuousTraffic): clears the
     * listener list of the receiver's live main ChangeStreamMonitor via reflection, simulating a
     * cursor that silently stops delivering events without the messaging noticing.
     *
     * @return the silenced monitor instance, for asserting it got replaced
     */
    private Object silenceMainCs(SingleCollectionMessaging messaging) throws Exception {
        Object monitor = mainCsMonitorOf(messaging);
        java.lang.reflect.Field listenersField = monitor.getClass().getDeclaredField("listeners");
        listenersField.setAccessible(true);
        ((java.util.Collection<?>) listenersField.get(monitor)).clear();
        return monitor;
    }

    private Object mainCsMonitorOf(SingleCollectionMessaging messaging) throws Exception {
        java.lang.reflect.Field monitorField = SingleCollectionMessaging.class.getDeclaredField("changeStreamMonitor");
        monitorField.setAccessible(true);
        return monitorField.get(messaging);
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
