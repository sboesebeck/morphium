package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Startup resilience for {@link DualChannelMessaging} (#265): a transient driver failure
 * during the pre-ready bootstrap (broken pooled connection after a wire hiccup, heartbeat
 * mid-reconnect) must NOT kill the messaging thread before readyLatch counts down - that
 * is exactly the CI failure shape "m4 not ready": ensureIndicesFor threw "Not connected",
 * the uncaught exception ended run(), and every waitForReady() caller timed out against
 * an instance that was dead on arrival. The pool heals underneath (heartbeat reconnect),
 * so the bootstrap has to retry, not die.
 */
@Tag("messaging")
public class DualChannelMessagingStartupResilienceTest extends MultiDriverTestBase {

    /** DM index bootstrap failing transiently, like a pooled connection mid-reconnect. */
    private static class FlakyStartupMessaging extends DualChannelMessaging {
        final AtomicInteger attempts = new AtomicInteger();

        @Override
        protected void ensureDmCollectionIndices() {
            if (attempts.incrementAndGet() <= 2) {
                throw new RuntimeException("simulated transient driver failure (Not connected)");
            }

            super.ensureDmCollectionIndices();
        }
    }

    /** Main/lock index bootstrap in init() failing transiently - the caller-thread twin. */
    private static class FlakyInitMessaging extends DualChannelMessaging {
        final AtomicInteger attempts = new AtomicInteger();

        @Override
        protected void ensureMessagingCollectionIndices() {
            if (attempts.incrementAndGet() <= 2) {
                throw new RuntimeException("simulated transient driver failure (Not connected)");
            }

            super.ensureMessagingCollectionIndices();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void initSurvivesTransientDriverFailures(Morphium morphium) throws Exception {
        try (morphium) {
            FlakyInitMessaging msg = new FlakyInitMessaging();
            msg.init(morphium); // must not throw despite two transient failures

            try {
                assertTrue(msg.attempts.get() >= 3, "the failing step must have been retried, attempts: "
                    + msg.attempts.get());
                msg.start();
                assertTrue(msg.waitForReady(20, TimeUnit.SECONDS), "instance must come up normally afterwards");
            } finally {
                msg.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void startupSurvivesTransientDriverFailures(Morphium morphium) throws Exception {
        try (morphium) {
            FlakyStartupMessaging msg = new FlakyStartupMessaging();
            msg.init(morphium);

            try {
                msg.start();
                assertTrue(msg.waitForReady(20, TimeUnit.SECONDS),
                    "startup must survive transient failures and still become ready (attempts: "
                    + msg.attempts.get() + ")");
                assertTrue(msg.attempts.get() >= 3, "the failing step must have been retried, attempts: "
                    + msg.attempts.get());
            } finally {
                msg.terminate();
            }
        }
    }
}
