package de.caluga.morphium.messaging;

import de.caluga.morphium.Morphium;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NetworkRegistry {
    private static final Logger log = LoggerFactory.getLogger(NetworkRegistry.class);

    private final Map<String, Set<String>> topicToListeners = new ConcurrentHashMap<>();
    private final Map<String, Long> participantLastSeen = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "network_registry");
        t.setDaemon(true);
        return t;
    });

    private final MorphiumMessaging messaging;
    private final long participantTimeout;

    public NetworkRegistry(MorphiumMessaging messaging) {
        this.messaging = messaging;
        this.participantTimeout = messaging.getMorphium().getConfig().messagingSettings().getMessagingRegistryParticipantTimeout();

        int updateInterval = messaging.getMorphium().getConfig().messagingSettings().getMessagingRegistryUpdateInterval();
        scheduler.scheduleAtFixedRate(this::runDiscovery, 0, updateInterval, TimeUnit.SECONDS);
    }

    private void runDiscovery() {
        // Send a broadcast status request message
        String statusTopic = messaging.getStatusInfoListenerName();
        Msg statusRequest = new Msg(statusTopic, "status_request", "all", 30000);
        statusRequest.setExclusive(false);
        messaging.sendMessage(statusRequest);

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

    @SuppressWarnings("unchecked")
    public void updateFrom(Msg statusResponse) {
        String senderId = statusResponse.getSender();
        if (senderId == null) {
            return;
        }

        participantLastSeen.put(senderId, System.currentTimeMillis());

        Map<String, Object> mapValue = statusResponse.getMapValue();
        if (mapValue == null || !mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey)) {
            return;
        }

        try {
            Map<String, Integer> listenersMap = (Map<String, Integer>) mapValue.get(StatusInfoListener.messageListenersbyNameKey);
            Set<String> topics = listenersMap.keySet();

            // First, remove this sender from all topics to handle cases where it stopped listening
            topicToListeners.values().forEach(listeners -> listeners.remove(senderId));

            // Now, add the sender to the topics it is currently listening to
            for (String topic : topics) {
                topicToListeners.computeIfAbsent(topic, k -> new ConcurrentHashMap<>().newKeySet()).add(senderId);
            }
        } catch (Exception e) {
            log.error("Failed to parse topics from status response from sender: {}", senderId, e);
        }
    }

    public boolean hasActiveListeners(String topicName) {
        Set<String> listeners = topicToListeners.get(topicName);
        return listeners != null && !listeners.isEmpty();
    }

    public boolean isParticipantActive(String senderId) {
        return participantLastSeen.containsKey(senderId);
    }

    public void terminate() {
        scheduler.shutdownNow();
    }
}
