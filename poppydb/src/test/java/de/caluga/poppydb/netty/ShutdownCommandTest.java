package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #356: {@code shutdown} was advertised by listCommands and could not work - it fell through to
 * the embedded driver, which answered "shutdown in memory not supported" while the server kept
 * running. The only way to stop a node was SIGTERM, so every rolling restart needed shell access
 * on every host. The handler now owns the command: it answers, then stops the node off the event
 * loop through the action PoppyDB wires in.
 */
public class ShutdownCommandTest {

    private InMemoryDriver drv;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private ElectionManager electionManager;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (electionManager != null) {
            electionManager.stop();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private MongoCommandHandler handler(boolean primary, ElectionManager em, BooleanSupplier syncing) {
        return new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "0.0.0.0", 27017, "my-rs", List.of("localhost:27017", "localhost:27018"),
                primary, "localhost:27017", 0, () -> null, em, syncing);
    }

    private Map<String, Object> send(EmbeddedChannel ch, Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("no reply for " + cmd.keySet().iterator().next()).isNotNull();
        return reply.getFirstDoc();
    }

    @Test
    public void withoutAWiredActionTheCommandIsRefusedNotFakedOrDroppedToTheDriver() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(true, null, () -> false));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).isEqualTo(115);
        assertThat(reply.get("errmsg").toString()).doesNotContain("in memory");
    }

    @Test
    public void answersFirstThenStopsTheNodeOffTheCallingThread() throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Thread> actionThread = new AtomicReference<>();
        AtomicBoolean releasedBeforeRun = new AtomicBoolean(false);
        Runnable action = () -> {
            actionThread.set(Thread.currentThread());

            try {
                // the test releases this only after it has read the reply - an action run
                // inline on the handler's thread would time out here instead
                releasedBeforeRun.set(go.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            done.countDown();
        };
        EmbeddedChannel ch = new EmbeddedChannel(handler(true, null, () -> false).setShutdownAction(action));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "$db", "admin"));
        go.countDown();

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(done.await(5, TimeUnit.SECONDS)).as("shutdown action never ran").isTrue();
        assertThat(actionThread.get())
                .as("PoppyDB.shutdown() awaits the event loops - run on the I/O thread it deadlocks")
                .isNotSameAs(Thread.currentThread());
        assertThat(releasedBeforeRun.get())
                .as("the reply must be out before the action runs, not after the node is gone")
                .isTrue();
    }

    @Test
    public void aRecoveringSecondaryCanStillBeStopped() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        EmbeddedChannel ch = new EmbeddedChannel(handler(false, null, () -> true)
                .setShutdownAction(done::countDown));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "$db", "admin"));

        assertThat(reply.get("ok"))
                .as("a re-syncing node is exactly the one you want to stop - 13436 would be wrong here: " + reply)
                .isEqualTo(1.0);
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void requiresAuthenticationWhenAuthIsEnabled() {
        AtomicBoolean called = new AtomicBoolean(false);
        EmbeddedChannel ch = new EmbeddedChannel(handler(true, null, () -> false)
                .setAuthRequired(true)
                .setShutdownAction(() -> called.set(true)));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "$db", "admin"));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat(((Number) reply.get("code")).intValue()).as("Unauthorized: " + reply).isEqualTo(13);
        assertThat(called.get()).as("an unauthenticated connection must not stop the node").isFalse();
    }

    @Test
    public void listCommandsAdvertisesShutdownOnlyWhenItWorks() {
        Map<String, Object> unwired = send(new EmbeddedChannel(handler(true, null, () -> false)),
                Doc.of("listCommands", 1, "$db", "admin"));
        Map<String, Object> wired = send(new EmbeddedChannel(handler(true, null, () -> false)
                .setShutdownAction(() -> { })), Doc.of("listCommands", 1, "$db", "admin"));

        @SuppressWarnings("unchecked")
        Map<String, Object> unwiredCommands = (Map<String, Object>) unwired.get("commands");
        @SuppressWarnings("unchecked")
        Map<String, Object> wiredCommands = (Map<String, Object>) wired.get("commands");
        assertThat(unwiredCommands).as("advertising a command that cannot work is how #356 started")
                .doesNotContainKey("shutdown");
        assertThat(wiredCommands).containsKey("shutdown");
    }

    private ElectionManager singleNodeLeader() throws Exception {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);
        electionManager = new ElectionManager("localhost:27017", List.of("localhost:27017"), config);
        CountDownLatch leader = new CountDownLatch(1);
        electionManager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leader.countDown();
            }
        });
        electionManager.start();
        assertThat(leader.await(5, TimeUnit.SECONDS)).as("single node did not elect itself").isTrue();
        return electionManager;
    }

    @Test
    public void aPrimaryStepsDownBeforeItStops() throws Exception {
        ElectionManager em = singleNodeLeader();
        CountDownLatch done = new CountDownLatch(1);
        EmbeddedChannel ch = new EmbeddedChannel(handler(true, em, () -> false)
                .setShutdownAction(done::countDown));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(em.isLeader())
                .as("without force the primary hands over first, otherwise a scripted rolling "
                    + "restart black-holes writes for a full election timeout")
                .isFalse();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void forceSkipsTheStepDown() throws Exception {
        ElectionManager em = singleNodeLeader();
        CountDownLatch done = new CountDownLatch(1);
        EmbeddedChannel ch = new EmbeddedChannel(handler(true, em, () -> false)
                .setShutdownAction(done::countDown));

        Map<String, Object> reply = send(ch, Doc.of("shutdown", 1, "force", true, "$db", "admin"));

        assertThat(reply.get("ok")).as("reply: " + reply).isEqualTo(1.0);
        assertThat(em.isLeader()).as("force means: stop now, no hand-over").isTrue();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
