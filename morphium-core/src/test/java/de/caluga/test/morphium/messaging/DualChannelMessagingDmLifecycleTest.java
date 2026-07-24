package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DM collection lifecycle coverage for {@link DualChannelMessaging} (#265, beta): the collection
 * (incl. its indices) exists once the instance is started, {@code terminate()} drops only its
 * own collection, and the registry-gated orphan sweep drops dead+empty collections while leaving
 * live ones alone.
 */
@Tag("messaging")
public class DualChannelMessagingDmLifecycleTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base, boolean registryEnabled) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
        cfg.messagingSettings().setMessagingRegistryEnabled(registryEnabled);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void dmCollectionExistsWithIndicesAfterStartTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, false);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging messaging = (DualChannelMessaging) m.createMessaging();

                try {
                    messaging.start();
                    assertTrue(messaging.waitForReady(30, TimeUnit.SECONDS));

                    String dmColl = messaging.getDMCollectionName();
                    assertTrue(m.listCollections().contains(dmColl),
                            "DM collection '" + dmColl + "' should exist after start()");

                    // TTL index on deleteAt must exist (ensureIndicesFor(Msg.class, ...) was called).
                    var indices = m.getIndexesFromMongo(dmColl);
                    assertNotNull(indices);
                    assertFalse(indices.isEmpty(), "DM collection should have indices (incl. TTL) after ensureIndicesFor");
                } finally {
                    messaging.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void terminateDropsOwnDmCollectionOnlyTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, false);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging a = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging b = (DualChannelMessaging) m.createMessaging();

                a.start();
                assertTrue(a.waitForReady(30, TimeUnit.SECONDS));
                b.start();
                assertTrue(b.waitForReady(30, TimeUnit.SECONDS));

                String dmA = a.getDMCollectionName();
                String dmB = b.getDMCollectionName();
                assertTrue(m.listCollections().contains(dmA));
                assertTrue(m.listCollections().contains(dmB));

                a.terminate();

                TestUtils.waitForConditionToBecomeTrue(10000, "a's DM collection should be dropped",
                        () -> !m.listCollections().contains(dmA));
                assertTrue(m.listCollections().contains(dmB), "b's DM collection must survive a's terminate()");

                b.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void orphanSweepDropsDeadEmptyLeavesLiveAloneTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, true);
            cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);
            cfg.messagingSettings().setMessagingDmCleanupOrphansOnStartup(true);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging live = (DualChannelMessaging) m.createMessaging();

                // Simulate an orphaned DM collection left behind by a long-gone instance: empty,
                // no corresponding running instance, no registry entry. Created BEFORE start() so
                // the very first sweep tick's listCollections() call (subject to Morphium's 15s
                // CollectionInfo cache, keyed by the exact pattern string) already observes it -
                // otherwise the first cache fill (from before this collection existed) would hide
                // it for up to 15s.
                String deadDmColl = live.getCollectionName() + "_dm_deadsender123";
                m.ensureIndicesFor(Msg.class, deadDmColl);
                assertTrue(m.listCollections().contains(deadDmColl));

                live.start();
                assertTrue(live.waitForReady(30, TimeUnit.SECONDS));
                // registry needs a listener to consider a participant "active" (see known
                // limitation documented in DualChannelMessaging#sweepOrphanDmCollections)
                live.addListenerForTopic("keepalive", (mm, msg) -> null);

                // give the registry time to learn about "live" via status broadcasts, then let
                // the sweep run at least once (same decouplePool tick as the fallback polls).
                TestUtils.waitForConditionToBecomeTrue(20000, "orphan sweep never dropped the dead collection",
                        () -> !m.listCollections().contains(deadDmColl));

                // the live instance's own (non-empty-irrelevant) DM collection must never be touched
                assertTrue(m.listCollections().contains(live.getDMCollectionName()));

                live.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void orphanSweepOffSwitchTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, true);
            cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);
            cfg.messagingSettings().setMessagingDmCleanupOrphansOnStartup(false);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging live = (DualChannelMessaging) m.createMessaging();
                live.start();
                assertTrue(live.waitForReady(30, TimeUnit.SECONDS));

                String deadDmColl = live.getCollectionName() + "_dm_deadsender456";
                m.ensureIndicesFor(Msg.class, deadDmColl);
                assertTrue(m.listCollections().contains(deadDmColl));

                // sweep disabled: the dead+empty collection must survive several scheduler ticks
                Thread.sleep(5000);
                assertTrue(m.listCollections().contains(deadDmColl),
                        "orphan sweep must not run when messagingDmCleanupOrphansOnStartup=false");

                live.terminate();
            }
        }
    }
}
