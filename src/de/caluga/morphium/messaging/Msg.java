package de.caluga.morphium.messaging;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PreStore;
import de.caluga.morphium.driver.MorphiumId;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:45
 * <p/>
 * Message class - used by Morphium's own messaging system<br>
 * </br>
 * Reads from any node, as this produces lots of reads! All Writes will block until <b>all nodes</b> have confirmed the
 * write!t
 */
@SuppressWarnings("WeakerAccess")
@Entity(polymorph = true)
@NoCache
//timeout <0 - setting relative to replication lag
//timeout == 0 - wait forever
@WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE, waitForJournalCommit = false)
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
@Lifecycle
@Index({"sender,locked_by,processed_by,recipient,priority,timestamp", "locked_by,processed_by,recipient,priority,timestamp",
        "sender,locked_by,processed_by,recipient,name,priority,timestamp"})
public class Msg {
    @Index
    private List<String> processedBy;
    @Id
    private MorphiumId msgId;
    @Index
    @UseIfnull
    private String lockedBy;
    @Index
    private long locked;
    private long ttl;
    private String sender;
    private String senderHost;
    private String recipient;
    @Transient
    private List<String> to;
    private Object inAnswerTo;
    //payload goes here
    private String name;
    private String msg;
    private List<Object> additional;
    private Map<String, Object> mapValue;
    private String value;
    @Index
    private long timestamp;
    @Index(options = "expireAfterSeconds:0")
    private Date deleteAt;

    private int priority=1000;
//    @Transient
//    private Boolean exclusive = false;

    public Msg() {
        // msgId = UUID.randomUUID().toString();
        lockedBy = "ALL";
//        exclusive = false;
    }

    public Msg(String name, String msg, String value) {
        this(name, msg, value, 30000, false);
    }

    public Msg(String name, String msg, String value, long ttl) {
        this(name, msg, value, ttl, false);
    }

    public Msg(String name, String msg, String value, long ttl, boolean exclusive) {
        this();
        this.name = name;
        this.msg = msg;
        this.value = value;
        this.ttl = ttl;
        setExclusive(exclusive);

    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @SuppressWarnings("unused")
    public boolean isExclusive() {
        return getLockedBy() == null || !getLockedBy().equals("ALL");
    }

    /**
     * if true (default) message can only be processed by one system at a time
     *
     * @param exclusive
     */
    public void setExclusive(boolean exclusive) {
        if (!exclusive) {
            lockedBy = "ALL";
        } else {
            lockedBy = null;
        }
    }

    @SuppressWarnings("unused")
    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    @SuppressWarnings("unused")
    public String getSenderHost() {
        return senderHost;
    }

    public void setSenderHost(String senderHost) {
        this.senderHost = senderHost;
    }

    @SuppressWarnings("unused")
    public Date getDeleteAt() {
        return deleteAt;
    }

    public void setDeleteAt(Date deleteAt) {
        this.deleteAt = deleteAt;
    }

    public void addRecipient(String id) {
        if (to == null) {
            to = new ArrayList<>();

        }
        if (!to.contains(id)) {
            to.add(id);
        }
    }

    @SuppressWarnings("unused")
    public void removeRecipient(String id) {
        if (to != null) {

            to.remove(id);
        }
    }

    public void addValue(String key, Object value) {
        if (mapValue == null) {
            mapValue = new HashMap<>();
        }
        mapValue.put(key, value);
    }

    @SuppressWarnings("unused")
    public void removeValue(String key) {
        if (mapValue == null) {
            return;
        }
        mapValue.remove(key);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> getMapValue() {
        return mapValue;
    }

    public void setMapValue(Map<String, Object> mapValue) {
        this.mapValue = mapValue;
    }

    public List<String> getTo() {
        return to;
    }

    public void setTo(List<String> to) {
        this.to = to;
    }

    public Object getInAnswerTo() {
        return inAnswerTo;
    }

    public void setInAnswerTo(Object inAnswerTo) {
        this.inAnswerTo = inAnswerTo;
    }

    public MorphiumId getMsgId() {
        return msgId;
    }

    public void setMsgId(MorphiumId msgId) {
        this.msgId = msgId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getProcessedBy() {
        return processedBy;
    }

    public void setProcessedBy(List<String> processedBy) {
        this.processedBy = processedBy;
    }

    public void addProcessedId(String id) {
        if (processedBy == null) {
            processedBy = new ArrayList<>();
        }
        processedBy.add(id);
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    @SuppressWarnings("unused")
    public long getLocked() {
        return locked;
    }

    public void setLocked(long locked) {
        this.locked = locked;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<Object> getAdditional() {
        return additional;
    }

    public void setAdditional(List<Object> additional) {
        this.additional = additional;
    }

    public void addAdditional(String value) {
        if (additional == null) {
            additional = new ArrayList<>();
        }
        additional.add(value);
    }

    @SuppressWarnings("unused")
    public void removeAdditional(String value) {
        if (additional == null) {
            return;
        }
        additional.remove(value);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Msg{" +
                " msgId='" + msgId + '\'' +
                ", inAnswerTo='" + inAnswerTo + '\'' +
                ", lockedBy='" + lockedBy + '\'' +
                ", locked=" + locked +
                ", ttl=" + ttl +
                ", sender='" + sender + '\'' +
                ", name='" + name + '\'' +
                ", msg='" + msg + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", additional='" + additional + '\'' +
                ", mapValue='" + mapValue + '\'' +
                ", recipient='" + recipient + '\'' +
                ", to_list='" + to + '\'' +

                ", processedBy=" + processedBy +
                '}';
    }

    @SuppressWarnings("unused")
    @PreStore
    public void preStore() {
        if (sender == null) {
            throw new RuntimeException("Cannot send msg anonymously - set Sender first!");
        }
        if (name == null) {
            throw new RuntimeException("Cannot send a message without name!");
        }
        if (ttl == 0) {
            LoggerFactory.getLogger(Msg.class).warn("Defaulting msg ttl to 30sec");
            ttl = 30000;
        }
//        if (!isExclusive()) {
//            locked = System.currentTimeMillis();
//            lockedBy = "ALL";
//        }
        if (deleteAt == null) {
            deleteAt = new Date(System.currentTimeMillis() + ttl);
        }
        timestamp = System.currentTimeMillis();
    }


    public Msg createAnswerMsg() {
        Msg ret = new Msg(name, msg, value, ttl);
        ret.setInAnswerTo(msgId);
        ret.addRecipient(sender);
        return ret;
    }

    public void sendAnswer(Messaging messaging, Msg m) {
        m.setInAnswerTo(this.msgId);
        //m.addRecipient(this.getSender());
        m.setRecipient(this.getSender());
        m.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl()));
        m.setMsgId(new MorphiumId());
        messaging.sendMessage(m);
    }


    public enum Fields {msgId, lockedBy, locked, ttl, sender, senderHost, recipient, to, inAnswerTo, name, msg, additional, mapValue, value, timestamp, deleteAt, priority, processedBy}
}
