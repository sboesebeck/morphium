package de.caluga.test.morphium.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MessagingParticipant;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

/**
 * Participants on one queue must all run the same messaging implementation - the collection
 * layouts differ and there is no bridge (#280). A mismatch used to fail silently ("most things
 * work, answers never arrive"). Every instance therefore announces its implementation in the
 * layout-independent participants collection and checks the others on startup: WARN by default,
 * THROW via {@link MessagingSettings.ImplementationCheck}.
 */
@Tag("messaging")
public class MessagingImplementationMismatchTest extends MultiDriverTestBase {

    /** default queue -> base collection "msg" -> participants collection "msg_participants" */
    private static final String PARTICIPANTS_COLL = "msg_participants";

    private MorphiumConfig configFor(Morphium base, String impl, MessagingSettings.ImplementationCheck check) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        // the two sides live in separate Morphium instances - for the inmem driver they must
        // explicitly share the database, otherwise each gets its own private storage (no-op for
        // real drivers, which share the database naturally)
        cfg.driverSettings().setInMemorySharedDatabases(true);
        cfg.messagingSettings().setMessagingImplementation(impl);
        cfg.messagingSettings().setMessagingImplementationCheck(check);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    private void clean(Morphium m) {
        m.dropCollection(Msg.class);
        m.dropCollection(MessagingParticipant.class, PARTICIPANTS_COLL, null);
        TestUtils.waitForConditionToBecomeTrue(5000, "participants collection not dropped",
            () -> m.createQueryFor(MessagingParticipant.class, PARTICIPANTS_COLL).countAll() == 0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void participantsAnnounceAndWithdraw(Morphium morphium) throws Exception {
        try (morphium) {
            try (Morphium m1 = new Morphium(configFor(morphium, SingleCollectionMessaging.NAME, MessagingSettings.ImplementationCheck.WARN));
                 Morphium m2 = new Morphium(configFor(morphium, DualChannelMessaging.NAME, MessagingSettings.ImplementationCheck.WARN))) {
                clean(m1);
                MorphiumMessaging standard = m1.createMessaging();
                MorphiumMessaging dual = m2.createMessaging();

                try {
                    standard.start();
                    assertTrue(standard.waitForReady(30, TimeUnit.SECONDS), "standard not ready");
                    // WARN (the default) must not prevent startup despite the mismatch
                    dual.start();
                    assertTrue(dual.waitForReady(30, TimeUnit.SECONDS), "dual not ready");

                    List<MessagingParticipant> participants =
                        m1.createQueryFor(MessagingParticipant.class, PARTICIPANTS_COLL).asList();
                    assertThat(participants).as("every instance announces itself").hasSize(2);
                    assertThat(participants).extracting(MessagingParticipant::getImplementation)
                        .containsExactlyInAnyOrder(SingleCollectionMessaging.NAME, DualChannelMessaging.NAME);
                    assertThat(participants).allSatisfy(p -> {
                        assertThat(p.getId()).isNotBlank();
                        assertThat(p.getLastSeen()).isGreaterThan(0);
                    });
                } finally {
                    standard.terminate();
                    dual.terminate();
                }

                TestUtils.waitForConditionToBecomeTrue(5000, "participants not withdrawn on terminate",
                    () -> m1.createQueryFor(MessagingParticipant.class, PARTICIPANTS_COLL).countAll() == 0);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void mismatchThrowsWhenConfigured(Morphium morphium) throws Exception {
        try (morphium) {
            for (String foreignImpl : List.of(DualChannelMessaging.NAME, MultiCollectionMessaging.NAME)) {
                try (Morphium m1 = new Morphium(configFor(morphium, SingleCollectionMessaging.NAME, MessagingSettings.ImplementationCheck.WARN));
                     Morphium m2 = new Morphium(configFor(morphium, foreignImpl, MessagingSettings.ImplementationCheck.THROW))) {
                    clean(m1);
                    MorphiumMessaging standard = m1.createMessaging();
                    MorphiumMessaging foreign = m2.createMessaging();

                    try {
                        standard.start();
                        assertTrue(standard.waitForReady(30, TimeUnit.SECONDS), "standard not ready");

                        assertThatThrownBy(foreign::start)
                            .as("a %s node joining a StandardMessaging queue must refuse to start with THROW", foreignImpl)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining(SingleCollectionMessaging.NAME);

                        // the refused instance must not have left its own announcement behind
                        List<MessagingParticipant> participants =
                            m1.createQueryFor(MessagingParticipant.class, PARTICIPANTS_COLL).asList();
                        assertThat(participants).extracting(MessagingParticipant::getImplementation)
                            .containsExactly(SingleCollectionMessaging.NAME);
                    } finally {
                        standard.terminate();
                        foreign.terminate();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sameImplementationPassesThrowCheck(Morphium morphium) throws Exception {
        try (morphium) {
            try (Morphium m1 = new Morphium(configFor(morphium, SingleCollectionMessaging.NAME, MessagingSettings.ImplementationCheck.THROW));
                 Morphium m2 = new Morphium(configFor(morphium, SingleCollectionMessaging.NAME, MessagingSettings.ImplementationCheck.THROW))) {
                clean(m1);
                MorphiumMessaging first = m1.createMessaging();
                MorphiumMessaging second = m2.createMessaging();

                try {
                    first.start();
                    assertTrue(first.waitForReady(30, TimeUnit.SECONDS), "first not ready");
                    // same implementation everywhere - THROW must not trigger
                    second.start();
                    assertTrue(second.waitForReady(30, TimeUnit.SECONDS), "second not ready");
                } finally {
                    first.terminate();
                    second.terminate();
                }
            }
        }
    }
}
