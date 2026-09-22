package de.caluga.test.poppydb.election;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #391: a node held back for a partial restore asks its peers for their restore finding and
 * lifts its guard once EVERY peer reports the same failed dump files, none holds a complete
 * copy, none knows a leader, and it is the highest-priority node among them. Anything unknown
 * keeps the guard (fail closed). Manager-level: the probe callback is answered synchronously
 * by the test in place of the network.
 */
@Tag("poppydb")
public class PartialRestoreClusterWideAcceptTest {

    private static final List<String> FILES = List.of("db_broken.morphium.gz");
    private static final String PEER_A = "peer-a:27017";
    private static final String PEER_B = "peer-b:27017";

    private final List<ElectionManager> managers = new ArrayList<>();
    private ListAppender<ILoggingEvent> logWatcher;

    @AfterEach
    public void tearDown() {
        for (ElectionManager m : managers) {
            try {
                m.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        managers.clear();
        if (logWatcher != null) {
            ((Logger) LoggerFactory.getLogger(ElectionManager.class)).detachAppender(logWatcher);
            logWatcher = null;
        }
    }

    private ElectionManager node(String address, int priority, int timeoutMs) {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(timeoutMs)
                .setElectionTimeoutMaxMs(timeoutMs)
                .setElectionPriority(priority);
        ElectionManager manager = new ElectionManager(address, List.of(address, PEER_A, PEER_B), config);
        managers.add(manager);
        return manager;
    }

    /** A held-back node whose probes are answered by {@code answers} (null = the peer stays silent). */
    private ElectionManager heldBackNode(String address, int priority, Function<String, RestoreStatus> answers,
            AtomicInteger voteRequestsSent) {
        ElectionManager manager = node(address, priority, 100);
        manager.setSendVoteRequest((peer, request) -> voteRequestsSent.incrementAndGet());
        manager.setSendRestoreStatusProbe(peer -> {
            RestoreStatus answer = answers.apply(peer);
            if (answer != null) {
                manager.handleRestoreStatusResponse(peer, answer);
            }
        });
        manager.setDataComplete(false);
        manager.setFailedRestoreFiles(FILES);
        return manager;
    }

    private static RestoreStatus sameLoss(String peer, int priority) {
        return new RestoreStatus(peer, false, FILES, priority, false);
    }

    private static void waitFor(AtomicInteger counter, long timeoutMs) throws InterruptedException {
        long until = System.currentTimeMillis() + timeoutMs;
        while (counter.get() == 0 && System.currentTimeMillis() < until) {
            Thread.sleep(25);
        }
    }

    @Test
    @Timeout(30)
    public void liftsTheGuardWhenEveryPeerReportsTheSameFailedFiles() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        AtomicReference<List<String>> accepted = new AtomicReference<>();
        ElectionManager manager = heldBackNode("me:27017", 100, peer -> sameLoss(peer, 50), sent);
        manager.setOnPartialRestoreAccepted(accepted::set);
        manager.start();

        waitFor(sent, 5000);

        assertTrue(sent.get() > 0,
                "every peer lost the same file and none knows a leader - nobody holds a better copy, "
                        + "so the highest-priority node must lift its guard and campaign");
        assertTrue(manager.isDataComplete(), "the guard must be lifted");
        assertTrue(manager.isPartialRestoreAccepted());
        long until = System.currentTimeMillis() + 2000;
        while (accepted.get() == null && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertEquals(FILES, accepted.get(), "the acceptance callback must carry the failed files");
    }

    @Test
    @Timeout(30)
    public void staysHeldBackWhenAPeerHoldsACompleteCopy() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100,
                peer -> peer.equals(PEER_A) ? new RestoreStatus(peer, true, List.of(), 50, false) : sameLoss(peer, 50),
                sent);
        manager.start();

        Thread.sleep(1500);   // many election timeouts / probe rounds

        assertEquals(0, sent.get(), "a peer with a complete copy has more than we do - the #306 guard stays");
        assertFalse(manager.isDataComplete());
        assertFalse(manager.isPartialRestoreAccepted());
    }

    @Test
    @Timeout(30)
    public void staysHeldBackWhenAPeerLostDifferentFiles() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100,
                peer -> peer.equals(PEER_B)
                        ? new RestoreStatus(peer, false, List.of("db_other.morphium.gz"), 50, false)
                        : sameLoss(peer, 50),
                sent);
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "a peer that lost a DIFFERENT file still holds ours - the guard stays");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void staysHeldBackWhenAPeerDoesNotAnswer() throws Exception {
        // an unreachable peer, or one from before this release that does not know the probe
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100,
                peer -> peer.equals(PEER_A) ? sameLoss(peer, 50) : null, sent);
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "a peer whose finding is unknown may hold the file - fail closed, "
                + "ALL peers must answer, not a majority");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void staysHeldBackWhenAPeerKnowsALeader() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100,
                peer -> peer.equals(PEER_B) ? new RestoreStatus(peer, false, FILES, 50, true) : sameLoss(peer, 50),
                sent);
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "with a leader around the sync path releases the guard - lifting it "
                + "would only produce a second primary");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void onlyTheHighestPriorityNodeLiftsItsGuard() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 50,
                peer -> sameLoss(peer, peer.equals(PEER_A) ? 100 : 25), sent);
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "peer-a (priority 100) outranks us (50) - it lifts its guard, we sync from it");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void equalPrioritiesAreBrokenByTheLowerAddress() throws Exception {
        // "z:27017" ranks below "peer-a"/"peer-b" at equal priority - holds
        AtomicInteger sentZ = new AtomicInteger();
        ElectionManager z = heldBackNode("z:27017", 50, peer -> sameLoss(peer, 50), sentZ);
        z.start();
        // "a:27017" ranks above both peers at equal priority - lifts
        AtomicInteger sentA = new AtomicInteger();
        ElectionManager a = heldBackNode("a:27017", 50, peer -> sameLoss(peer, 50), sentA);
        a.start();

        waitFor(sentA, 5000);
        Thread.sleep(500);

        assertTrue(sentA.get() > 0, "at equal priority the lowest address lifts its guard");
        assertEquals(0, sentZ.get(), "and the others stay held back");
        assertFalse(z.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void incompleteDataWithoutAFailedFileSetNeverResolvesByItself() throws Exception {
        // a store emptied for a sync, or a restore that threw: nobody can say what is missing
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = node("me:27017", 100, 100);
        manager.setSendVoteRequest((peer, request) -> sent.incrementAndGet());
        manager.setSendRestoreStatusProbe(peer ->
                manager.handleRestoreStatusResponse(peer, new RestoreStatus(peer, false, List.of(), 50, false)));
        manager.setDataComplete(false);   // no failed files recorded
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "incomplete for an unknown reason must never be accepted automatically");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void answersAProbeWithItsOwnFinding() throws Exception {
        ElectionManager manager = node("me:27017", 75, 60_000);
        manager.setDataComplete(false);
        manager.setFailedRestoreFiles(FILES);
        manager.start();

        RestoreStatus status = manager.restoreStatus();
        assertEquals("me:27017", status.getNodeId());
        assertFalse(status.isDataComplete());
        assertEquals(FILES, status.getFailedFiles());
        assertEquals(75, status.getPriority());
        assertFalse(status.isLeaderKnown(), "cold start: no leader heard");

        // a heartbeat from a leader is reported as leaderKnown
        manager.handleAppendEntries(AppendEntriesRequest.heartbeat(manager.getCurrentTerm(), PEER_A, 0, 0, 0));
        assertTrue(manager.restoreStatus().isLeaderKnown());

        // wire roundtrip keeps every field; an error answer parses to null (unknown)
        RestoreStatus parsed = RestoreStatus.fromMap(status.toMap());
        assertNotNull(parsed);
        assertEquals(status.toString(), parsed.toString());
        assertEquals(null, RestoreStatus.fromMap(Map.of("ok", 0.0, "errmsg", "no such command")));
    }

    @Test
    @Timeout(30)
    public void operatorCommandLiftsTheGuardAtOnce() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        // peers never answer - the automatic path cannot resolve this
        ElectionManager manager = heldBackNode("me:27017", 100, peer -> null, sent);
        AtomicReference<List<String>> accepted = new AtomicReference<>();
        manager.setOnPartialRestoreAccepted(accepted::set);
        manager.start();
        Thread.sleep(500);
        assertEquals(0, sent.get(), "still held back");

        assertTrue(manager.acceptPartialRestore("test"), "the operator lifts the guard");
        assertTrue(manager.isDataComplete());
        assertTrue(manager.isPartialRestoreAccepted());
        assertFalse(manager.acceptPartialRestore("test"), "a second call has nothing to lift");

        waitFor(sent, 5000);
        assertTrue(sent.get() > 0, "and the node campaigns on its next timeout");
        long until = System.currentTimeMillis() + 2000;
        while (accepted.get() == null && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertEquals(FILES, accepted.get());
    }

    @Test
    @Timeout(30)
    public void operatorCommandRefusesWithoutAFailedFileSet() throws Exception {
        ElectionManager manager = node("me:27017", 100, 60_000);
        manager.setDataComplete(false);   // emptied for a sync - not a restore finding
        manager.start();

        assertFalse(manager.acceptPartialRestore("test"),
                "an emptied store must not be promoted by the command - only a failed restore is accepted");
        assertFalse(manager.isDataComplete());
    }

    @Test
    @Timeout(30)
    public void holdBackWarnNamesTheFilesAndIsThrottled() throws Exception {
        logWatcher = new ListAppender<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger(ElectionManager.class)).addAppender(logWatcher);

        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100, peer -> null, sent);
        manager.start();

        Thread.sleep(2000);   // ~20 election timeouts

        List<String> warns = logWatcher.list.stream()
                .filter(ev -> ev.getLevel().isGreaterOrEqual(Level.WARN))
                .map(ILoggingEvent::getFormattedMessage)
                .filter(msg -> msg.contains("holding back candidacy"))
                .toList();
        assertEquals(1, warns.size(),
                "the hold-back WARN must appear once and then at most once a minute, got " + warns.size());
        assertTrue(warns.get(0).contains("db_broken.morphium.gz"),
                "the WARN must name the failed dump files, got: " + warns.get(0));
    }

    @Test
    @Timeout(30)
    public void aNewRoundForgetsOldAnswers() throws Exception {
        // peer-b answers only the FIRST probe; a later round must not reuse that answer
        Map<String, AtomicInteger> probes = new ConcurrentHashMap<>();
        AtomicInteger sent = new AtomicInteger();
        ElectionManager manager = heldBackNode("me:27017", 100, peer -> {
            int n = probes.computeIfAbsent(peer, p -> new AtomicInteger()).incrementAndGet();
            if (peer.equals(PEER_B)) {
                return n == 1 ? sameLoss(peer, 50) : null;
            }
            // peer-a answers late: only from the second round on
            return n >= 2 ? sameLoss(peer, 50) : null;
        }, sent);
        manager.start();

        Thread.sleep(1500);

        assertEquals(0, sent.get(), "peer-b's answer from round 1 must not combine with peer-a's from round 2");
        assertFalse(manager.isDataComplete());
    }
}
