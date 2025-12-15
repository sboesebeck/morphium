package de.caluga.morphium.messaging;

import de.caluga.morphium.Morphium;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MessagingRegistry {
    private static final Logger log = LoggerFactory.getLogger(MessagingRegistry.class);

    private final Map<String, Set<String>> topicToListeners = new ConcurrentHashMap<>();
    private final Map<String, Long> participantLastSeen = new ConcurrentHashMap<>();
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final Object discoveryLock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "network_registry");
        t.setDaemon(true);
        return t;
    });

    private final MorphiumMessaging messaging;
    private final long participantTimeout;
    private volatile boolean terminated = false;

    public MessagingRegistry(MorphiumMessaging messaging) {
        this.messaging = messaging;
        this.participantTimeout = messaging.getMorphium().getConfig().messagingSettings().getMessagingRegistryParticipantTimeout();

        int updateInterval = messaging.getMorphium().getConfig().messagingSettings().getMessagingRegistryUpdateInterval();
        scheduler.scheduleAtFixedRate(this::runDiscovery, 0, updateInterval, TimeUnit.SECONDS);
    }

    private void runDiscovery() {
        if (terminated) {
            return;
        }

        Morphium morphium = null;
        try {
            morphium = messaging.getMorphium();
        } catch (Exception ignored) {
        }

        // During shutdown Morphium can already be partially torn down (config/driver nulled).
        if (morphium == null || morphium.getConfig() == null || morphium.getDriver() == null || !messaging.isRunning()) {
            return;
        }

        synchronized (discoveryLock) {
            // Send a broadcast status request message
            String statusTopic = messaging.getStatusInfoListenerName();
            Msg statusRequest = new Msg(statusTopic, "status_request", "all", 30000);
            statusRequest.setExclusive(false);
            try {
                if (terminated) {
                    return;
                }
                messaging.sendMessage(statusRequest);
            } catch (Exception e) {
                // Discovery should never break normal messaging flows.
                log.debug("Failed to send status request during discovery", e);
            }

            // Prune inactive participants
            long now = System.currentTimeMillis();
            participantLastSeen.entrySet().removeIf(entry -> {
                boolean isStale = now - entry.getValue() > participantTimeout;
                if (isStale) {
                    log.info("Removing stale participant: {}", entry.getKey());
                    // Remove this participant from all topic listener lists
                    topicToListeners.values().forEach(listeners -> listeners.remove(entry.getKey()));
                }
                return isStale;
            });

            // Remove topics with no listeners
            topicToListeners.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    public void updateFrom(Msg statusResponse) {
        String senderId = statusResponse.getSender();
        if (senderId == null) {
            return;
        }

        Map<String, Object> mapValue = statusResponse.getMapValue();
        if (mapValue == null || !mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey)) {
            return;
        }

        try {
            Map<String, Integer> listenersMap = (Map<String, Integer>) mapValue.get(StatusInfoListener.messageListenersbyNameKey);
            Set<String> topics = listenersMap.keySet();

            if (topics.isEmpty()) {
                // A participant reporting no listeners is considered inactive.
                participantLastSeen.remove(senderId);
                topicToListeners.values().forEach(listeners -> listeners.remove(senderId));
                return;
            }

            participantLastSeen.put(senderId, System.currentTimeMillis());

            // First, remove this sender from all topics to handle cases where it stopped listening
            topicToListeners.values().forEach(listeners -> listeners.remove(senderId));

            // Now, add the sender to the topics it is currently listening to
            for (String topic : topics) {
                topicToListeners.computeIfAbsent(topic, k -> new ConcurrentHashMap<>().newKeySet()).add(senderId);
            }

            log.info("Registry update from {} - topics {}", senderId, topics);
            lastUpdateTimestamp.set(System.currentTimeMillis());
        } catch (Exception e) {
            log.error("Failed to parse topics from status response from sender: {}", senderId, e);
        }
    }

    public boolean hasActiveListeners(String topicName) {
        Set<String> listeners = topicToListeners.get(topicName);
        if (listeners == null || listeners.isEmpty()) {
            log.warn("No registry listeners for topic {} on sender {}. Known topics: {}", topicName, messaging.getSenderId(), topicToListeners.keySet());
            return false;
        }

        return true;
    }

    public boolean isParticipantActive(String senderId) {
        Long lastSeen = participantLastSeen.get(senderId);
        if (lastSeen == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now - lastSeen > participantTimeout) {
            participantLastSeen.remove(senderId);
            topicToListeners.values().forEach(listeners -> listeners.remove(senderId));
            return false;
        }
        boolean hasAnyTopic = topicToListeners.values().stream().anyMatch(listeners -> listeners.contains(senderId));
        return hasAnyTopic;
    }

    public void triggerDiscoveryAndWait(long timeoutMs) {
        long previous = lastUpdateTimestamp.get();
        runDiscovery();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            if (lastUpdateTimestamp.get() > previous) {
                return;
            }

            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void terminate() {
        terminated = true;
        scheduler.shutdownNow();
    }
}
