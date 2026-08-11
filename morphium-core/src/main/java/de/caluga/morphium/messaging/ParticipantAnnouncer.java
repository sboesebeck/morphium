package de.caluga.morphium.messaging;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.config.MessagingSettings;

/**
 * Announces a messaging instance in the layout-independent {@code <queue>_participants}
 * collection and checks what implementation the other participants on the queue run (#280).
 *
 * <p>The check cannot run over the messaging channel itself: between two implementations with
 * disjoint collection layouts (e.g. Standard vs. MultiCollection) no status message ever crosses
 * over, so a registry-based check would be blind in exactly the broken case. The participants
 * collection is derived from the queue NAME alone and therefore shared by every implementation.
 *
 * <p>Detection and diagnostics only - no bridging, no adoption of the other side's layout.
 * Behaviour per {@link MessagingSettings.ImplementationCheck}: WARN (default) logs, THROW
 * refuses startup, IGNORE skips announcement and check entirely.
 */
class ParticipantAnnouncer {
    private static final Logger log = LoggerFactory.getLogger(ParticipantAnnouncer.class);

    private final Morphium morphium;
    private final MorphiumMessaging owner;
    private final MessagingSettings settings;
    private final String implementationName;
    private final String collectionName;
    private final long startedAt = System.currentTimeMillis();
    /** mismatched participant ids already warned about - each offender is logged once */
    private final Set<String> warnedAbout = ConcurrentHashMap.newKeySet();
    private ScheduledExecutorService heartbeat;

    ParticipantAnnouncer(Morphium morphium, MorphiumMessaging owner, MessagingSettings settings,
                         String implementationName) {
        this.morphium = morphium;
        this.owner = owner;
        this.settings = settings;
        this.implementationName = implementationName;
        this.collectionName = participantsCollectionName(owner.getQueueName());
    }

    /**
     * Same base-name derivation as the Standard/DualChannel message collection ("msg" /
     * "mmsg_&lt;queue&gt;") so the name is a function of the QUEUE, not of any implementation's
     * layout - MultiCollectionMessaging keys its message collections differently but must land
     * in the same participants collection.
     */
    static String participantsCollectionName(String queueName) {
        // "msg" is MessagingSettings' default queue name; Standard/DualChannel report the
        // default queue as null while MultiCollection reports the literal default - all three
        // MUST land in the same collection or the check is blind exactly across implementations.
        String base = (queueName == null || queueName.isEmpty() || queueName.equals("msg"))
                      ? "msg" : "mmsg_" + queueName;
        return base + "_participants";
    }

    /**
     * Announce this instance and check the other participants. Called synchronously from
     * {@code start()} BEFORE the messaging threads spin up, so ImplementationCheck.THROW can
     * abort startup cleanly (the own announcement is withdrawn again in that case).
     *
     * @throws IllegalStateException on a mismatch with ImplementationCheck.THROW
     */
    void announceAndCheck() {
        if (settings.getMessagingImplementationCheck() == MessagingSettings.ImplementationCheck.IGNORE) {
            return;
        }

        announce();
        List<MessagingParticipant> foreign = freshForeignParticipants();

        if (!foreign.isEmpty()) {
            String msg = "Messaging implementation mismatch on queue '" + owner.getCollectionName()
                + "': this instance runs " + implementationName + ", but other participants run "
                + describe(foreign) + ". The collection layouts are not interoperable - answers and "
                + "directed messages between mismatched participants are lost silently (#280).";

            if (settings.getMessagingImplementationCheck() == MessagingSettings.ImplementationCheck.THROW) {
                withdraw();
                throw new IllegalStateException(msg);
            }

            log.warn(msg);
            foreign.forEach(p -> warnedAbout.add(p.getId()));
        }

        long interval = Math.max(1, settings.getMessagingRegistryUpdateInterval());
        heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "msg-participant-" + owner.getSenderId());
            t.setDaemon(true);
            return t;
        });
        heartbeat.scheduleWithFixedDelay(this::heartbeatTick, interval, interval, TimeUnit.SECONDS);
    }

    /** Stop the heartbeat and withdraw this instance's announcement (called from terminate()). */
    void shutdown() {
        if (heartbeat != null) {
            heartbeat.shutdownNow();
            heartbeat = null;
        }
        withdraw();
    }

    private void heartbeatTick() {
        try {
            announce();
            cleanupStale();
            // Late joiners with a mismatched implementation can only be WARNed about - throwing
            // on a background thread would reach nobody. Each offender is logged once.
            for (MessagingParticipant p : freshForeignParticipants()) {
                if (warnedAbout.add(p.getId())) {
                    log.warn("Messaging implementation mismatch on queue '{}': participant {} runs {}, "
                             + "this instance runs {} - traffic between the two is lost silently (#280)",
                             owner.getCollectionName(), p.getId(), p.getImplementation(), implementationName);
                }
            }
        } catch (Exception e) {
            // heartbeat must never kill its scheduler - next tick retries
            log.debug("participant heartbeat failed: {}", e.getMessage());
        }
    }

    private void announce() {
        MessagingParticipant p = new MessagingParticipant();
        p.setId(owner.getSenderId());
        p.setImplementation(implementationName);
        p.setHostname(hostname());
        p.setStartedAt(startedAt);
        p.setLastSeen(System.currentTimeMillis());
        morphium.store(p, collectionName);
    }

    private void withdraw() {
        try {
            MessagingParticipant p = new MessagingParticipant();
            p.setId(owner.getSenderId());
            morphium.delete(p, collectionName);
        } catch (Exception e) {
            log.debug("could not withdraw participant announcement: {}", e.getMessage());
        }
    }

    private List<MessagingParticipant> freshForeignParticipants() {
        long cutoff = System.currentTimeMillis() - settings.getMessagingRegistryParticipantTimeout();
        return participants().stream()
               .filter(p -> !owner.getSenderId().equals(p.getId()))
               .filter(p -> p.getLastSeen() >= cutoff)
               .filter(p -> !implementationName.equals(p.getImplementation()))
               .collect(Collectors.toList());
    }

    /** Dead instances leave a document per restart behind - prune anything long past the timeout. */
    private void cleanupStale() {
        long cutoff = System.currentTimeMillis() - 3 * settings.getMessagingRegistryParticipantTimeout();
        for (MessagingParticipant p : participants()) {
            if (p.getLastSeen() < cutoff) {
                morphium.delete(p, collectionName);
            }
        }
    }

    private List<MessagingParticipant> participants() {
        return morphium.createQueryFor(MessagingParticipant.class, collectionName).asList();
    }

    private static String describe(List<MessagingParticipant> participants) {
        return participants.stream()
               .map(p -> p.getId() + " (" + p.getImplementation() + " on " + p.getHostname() + ")")
               .collect(Collectors.joining(", "));
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
