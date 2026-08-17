package de.caluga.test.morphium.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The listener map is published lock-free to the poll thread, so it may only be written by
 * clone-and-swap - but that read-modify-write was unsynchronized. Two writers racing (two
 * threads registering listeners, or an application thread registering while the freshly
 * started messaging thread installs the status-info listener) both clone the same map and the
 * later swap silently drops the other's entry: the listener is gone and its messages are never
 * delivered. The visible symptom was an NPE in addListenerForTopic when the entry vanished
 * between the contains() check and the add().
 */
@Tag("core")
public class MessagingListenerRegistrationRaceTest {

    private Morphium morphium;
    private final List<MorphiumMessaging> toTerminate = new ArrayList<>();

    private Morphium createMorphium(String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        return new Morphium(cfg);
    }

    private MorphiumMessaging create(String impl) {
        MorphiumMessaging m = impl.equals("single") ? new SingleCollectionMessaging() : new DualChannelMessaging();
        m.init(morphium);
        toTerminate.add(m);
        return m;
    }

    @AfterEach
    public void teardown() {
        for (MorphiumMessaging m : toTerminate) {
            try {
                m.terminate();
            } catch (Exception e) {
                // teardown must not mask the assertion failure
            }
        }
        toTerminate.clear();
        if (morphium != null) {
            morphium.close();
            morphium = null;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"single", "dual"})
    @Timeout(60)
    public void concurrentRegistrationKeepsEveryListener(String impl) throws Exception {
        morphium = createMorphium("msg_listener_race_" + impl);
        MorphiumMessaging messaging = create(impl);

        int threads = 8;
        int topicsPerThread = 25;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        MessageListener listener = (msg, m) -> null;

        for (int t = 0; t < threads; t++) {
            final int threadNo = t;
            new Thread(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < topicsPerThread; i++) {
                        messaging.addListenerForTopic("topic_" + threadNo + "_" + i, listener);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }, "register-" + t).start();
        }

        startGate.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).as("all registration threads finished").isTrue();
        assertThat(failure.get()).as("registration must not throw").isNull();

        var registered = messaging.getListenerNames();
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < topicsPerThread; i++) {
                assertThat(registered)
                        .as("listener registered concurrently must survive competing registrations")
                        .containsKey("topic_" + t + "_" + i);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"single", "dual"})
    @Timeout(60)
    public void registrationSurvivesConcurrentRemovalOnSameTopic(String impl) throws Exception {
        morphium = createMorphium("msg_listener_addremove_race_" + impl);
        MorphiumMessaging messaging = create(impl);

        // Removing the last listener of a topic drops the whole topic entry. A registration
        // that looked up the topic's listener list just before that must not end up adding to
        // the orphaned list - its listener would be silently lost. Each topic offers exactly
        // one shot at that window (once the keeper is in, the entry can no longer go empty),
        // so the two threads are re-synchronized per topic to keep them colliding.
        int topics = 20_000;
        CyclicBarrier perTopic = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        MessageListener keeper = (msg, m) -> null;
        MessageListener transientListener = (msg, m) -> null;

        new Thread(() -> {
            try {
                for (int i = 0; i < topics; i++) {
                    perTopic.await(5, TimeUnit.SECONDS);
                    messaging.addListenerForTopic("topic_" + i, keeper);
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        }, "keeper").start();

        new Thread(() -> {
            try {
                for (int i = 0; i < topics; i++) {
                    perTopic.await(5, TimeUnit.SECONDS);
                    messaging.addListenerForTopic("topic_" + i, transientListener);
                    messaging.removeListenerForTopic("topic_" + i, transientListener);
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        }, "add-remove").start();

        assertThat(done.await(50, TimeUnit.SECONDS)).as("both threads finished").isTrue();
        assertThat(failure.get()).as("neither thread may throw").isNull();

        var registered = messaging.getListenerNames();
        for (int i = 0; i < topics; i++) {
            assertThat(registered)
                    .as("a listener that was never removed must still be registered")
                    .containsKey("topic_" + i);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"single", "dual"})
    @Timeout(60)
    public void statusInfoListenerCoexistsWithAnApplicationListenerOfTheSameName(String impl) throws Exception {
        morphium = createMorphium("msg_listener_statusinfo_coexist_" + impl);
        MorphiumMessaging messaging = create(impl);
        MessageListener listener = (msg, m) -> null;
        String statusName = messaging.getStatusInfoListenerName();

        // Installing/removing the status-info listener must not take an application listener
        // registered under the same name with it - it only adds and removes itself.
        messaging.addListenerForTopic(statusName, listener);
        messaging.setStatusInfoListenerEnabled(true);
        assertThat(messaging.getListenerNames()).containsKey(statusName);
        assertThat(messaging.getListenerNames().get(statusName))
                .as("application listener must survive the status-info install")
                .contains(listener.getClass().getName());

        messaging.setStatusInfoListenerEnabled(false);
        assertThat(messaging.getListenerNames())
                .as("disabling the status-info listener must not drop the application listener")
                .containsKey(statusName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"single", "dual"})
    @Timeout(60)
    public void statusInfoListenerInstallDoesNotDropTopicListeners(String impl) throws Exception {
        morphium = createMorphium("msg_listener_statusinfo_race_" + impl);
        MorphiumMessaging messaging = create(impl);

        // Same clone-and-swap on the listener map as the messaging thread performs in run()
        // when it installs the status-info listener - reachable here without the startup race.
        int topics = 200;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        MessageListener listener = (msg, m) -> null;

        new Thread(() -> {
            try {
                startGate.await();
                for (int i = 0; i < topics; i++) {
                    messaging.addListenerForTopic("topic_" + i, listener);
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        }, "register").start();

        new Thread(() -> {
            try {
                startGate.await();
                for (int i = 0; i < topics; i++) {
                    messaging.setStatusInfoListenerEnabled(i % 2 == 0);
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        }, "statusinfo-toggle").start();

        startGate.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).as("both threads finished").isTrue();
        assertThat(failure.get()).as("neither registration nor status-info toggle may throw").isNull();

        var registered = messaging.getListenerNames();
        for (int i = 0; i < topics; i++) {
            assertThat(registered)
                    .as("topic listener must survive a concurrent status-info listener install")
                    .containsKey("topic_" + i);
        }
    }

    @Test
    @Timeout(60)
    public void removingAnUnregisteredListenerLeavesTheOthersAlone() throws Exception {
        morphium = createMorphium("msg_listener_mcm_remove");
        MorphiumMessaging messaging = new MultiCollectionMessaging();
        messaging.init(morphium);
        toTerminate.add(messaging);
        messaging.start();

        MessageListener registered = (msg, m) -> null;
        MessageListener neverRegistered = (msg, m) -> null;
        messaging.addListenerForTopic("keep_me", registered);

        messaging.removeListenerForTopic("keep_me", neverRegistered);

        assertThat(messaging.getListenerNames())
                .as("removing a listener that was never registered must not evict a different one")
                .containsKey("keep_me");
    }

    @Test
    @Timeout(60)
    public void removingAListenerFromAnUnknownTopicDoesNotThrow() throws Exception {
        morphium = createMorphium("msg_listener_mcm_remove_unknown");
        MorphiumMessaging messaging = new MultiCollectionMessaging();
        messaging.init(morphium);
        toTerminate.add(messaging);
        messaging.start();

        messaging.removeListenerForTopic("never_registered_topic", (msg, m) -> null);
    }
}
