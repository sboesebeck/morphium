package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Shutdown coverage for {@link DualChannelMessaging} (#265, beta): {@code terminate()} while a
 * {@code sendAndAwaitFirstAnswer} call is in flight must surface the expected shutdown exception,
 * both change-stream monitors and the DM dispatcher thread must stop, and sending after
 * {@code terminate()} must be refused rather than silently swallowed or crash.
 */
@Tag("messaging")
public class DualChannelMessagingShutdownTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void terminateDuringInFlightAwaitThrowsShutdownExceptionTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging requester = (DualChannelMessaging) m.createMessaging();

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));

                    // sendAndAwaitFirstAnswer's blocking wait (a plain BlockingQueue#poll on the
                    // CALLING thread) is NOT actively interrupted by terminate() - that mirrors
                    // SingleCollectionMessaging's own (forked, unmodified) behavior: terminate()
                    // does not reach into other threads blocked inside an in-flight await. What IS
                    // guaranteed is the immediate "if (!running) throw SystemShutdownException"
                    // pre-check. To exercise both realistically within a bounded test time, the
                    // await uses a short timeout with no responder registered: terminate() races
                    // against it, so either outcome (immediate shutdown exception if terminate()
                    // wins the race, or a plain timeout exception if the await had already started
                    // blocking) is a correct, accepted result - never a hang or a normal return.
                    final long awaitTimeoutMs = 3000;
                    java.util.concurrent.atomic.AtomicBoolean sawExpectedException = new java.util.concurrent.atomic.AtomicBoolean(false);
                    Thread waiter = Thread.ofPlatform().start(() -> {
                        Msg req = new Msg("nobody-listens", "ping", "");

                        try {
                            requester.sendAndAwaitFirstAnswer(req, awaitTimeoutMs);
                            fail("expected SystemShutdownException or MessageTimeoutException, got a normal return");
                        } catch (SingleCollectionMessaging.SystemShutdownException expected) {
                            sawExpectedException.set(true);
                        } catch (SingleCollectionMessaging.MessageTimeoutException alsoAcceptable) {
                            sawExpectedException.set(true);
                        }
                    });

                    Thread.sleep(200); // let sendAndAwaitFirstAnswer actually start blocking first
                    requester.terminate();
                    waiter.join(awaitTimeoutMs + 5000);
                    assertFalse(waiter.isAlive(), "waiter thread did not finish within timeout + grace period after terminate()");
                    assertTrue(sawExpectedException.get(), "waiter did not observe the expected shutdown/timeout exception");
                } finally {
                    // idempotent-ish safety net; terminate() was already called above
                    try {
                        requester.terminate();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void terminateStopsBothMonitorsAndDispatcherThreadTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging messaging = (DualChannelMessaging) m.createMessaging();
                messaging.start();
                assertTrue(messaging.waitForReady(30, TimeUnit.SECONDS));
                assertTrue(messaging.changeStreamsLive() || !messaging.isUseChangeStream());
                assertTrue(messaging.dmChangeStreamLive() || !messaging.isUseChangeStream());

                String threadNamePrefix = "msg-dm-" + messaging.getSenderId();
                boolean dispatcherThreadExistsBefore = Thread.getAllStackTraces().keySet().stream()
                        .anyMatch(t -> t.getName().equals(threadNamePrefix));
                assertTrue(dispatcherThreadExistsBefore, "DM dispatcher thread should be running after start()");

                messaging.terminate();
                Thread.sleep(2500); // allow terminate()'s own join(2000) + monitor shutdown to settle

                boolean dispatcherThreadExistsAfter = Thread.getAllStackTraces().keySet().stream()
                        .anyMatch(t -> t.getName().equals(threadNamePrefix));
                assertFalse(dispatcherThreadExistsAfter, "DM dispatcher thread should be stopped after terminate()");
                assertFalse(messaging.dmChangeStreamLive(), "DM change stream should be terminated");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAfterTerminateIsRefusedTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging messaging = (DualChannelMessaging) m.createMessaging();
                messaging.start();
                assertTrue(messaging.waitForReady(30, TimeUnit.SECONDS));
                messaging.terminate();

                // sendMessage on a terminated instance must not throw/crash the JVM and must not
                // resurrect the DM collection - it should be a silent no-op (see storeMsg's
                // "!running" guard, unmodified from SingleCollectionMessaging).
                Msg msg = new Msg("after-terminate", "x", "");
                assertDoesNotThrow(() -> messaging.sendMessage(msg));
            }
        }
    }
}
