package de.caluga.morphium.messaging;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;

/**
 * One heartbeat document per live messaging instance in the layout-independent
 * {@code <queue>_participants} collection (#280). Every implementation writes the same shape
 * here regardless of how it lays out its message collections, so an implementation mismatch on
 * one queue can be detected even between participants that share no message collection at all -
 * which is exactly the case that fails silently over the messaging channel itself.
 *
 * <p>Written and read by {@code ParticipantAnnouncer}; the {@code _id} is the instance's
 * messaging sender id, so re-announcing (heartbeat) is a plain store/replace.
 */
@Entity(typeId = "msg_participant")
@NoCache
public class MessagingParticipant {
    @Id
    private String id;
    private String implementation;
    private String hostname;
    private long startedAt;
    private long lastSeen;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    @Override
    public String toString() {
        return "MessagingParticipant{id=" + id + ", implementation=" + implementation
               + ", hostname=" + hostname + ", lastSeen=" + lastSeen + "}";
    }
}
