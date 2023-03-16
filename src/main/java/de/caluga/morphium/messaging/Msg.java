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
@SuppressWarnings({"WeakerAccess", "UnusedReturnValue", "CommentedOutCode"})
@Entity(polymorph = true, typeId = "msg")
@NoCache
//timeout <0 - setting relative to replication lag
//timeout == 0 - wait forever
@WriteSafety(level = SafetyLevel.MAJORITY)
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
@Lifecycle
@Index({
    "sender,processed_by,in_answer_to",
    "sender,processed_by",
    "msgId,sender,processed_by,name,priority,timestamp",
    "msgId,processed_by,name",
    "processed_by,priority,timestamp"
    // "sender,processed_by,in_answer_to",
    // "sender,locked_by,processed_by",
    // "locked_by","locked",
    // "msgId,sender,locked_by,processed_by,name,priority,timestamp",
    // "msgId,locked_by,processed_by,name",
    // "locked_by,processed_by,priority,timestamp"
})
public class Msg {
    @Index
    private List<String> processedBy;
    @Id
    private MorphiumId msgId;
    @Index
    // @UseIfnull
    // private String lockedBy;
    // @Index
    // private long locked;
    private long ttl;
    private String sender;
    private String senderHost;
    private List<String> recipients;
    private MorphiumId inAnswerTo;
    //payload goes here
    @Index
    private String name;
    private String msg;
    private List<Object> additional;
    private Map<String, Object> mapValue;
    private String value;
    @Index
    private long timestamp;
    @Index(options = "expireAfterSeconds:0")
    private Date deleteAt;
    private boolean timingOut = true;
    private boolean deleteAfterProcessing = false;
    private int deleteAfterProcessingTime = 0;
    private int priority = 1000;
    private Boolean exclusive = false;

    public Msg() {
        // msgId = UUID.randomUUID().toString();
        // lockedBy = "ALL";
        exclusive = false;
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
    public boolean isTimingOut() {
        return timingOut;
    }

    public Msg setTimingOut(boolean tm) {
        timingOut = tm;
        return this;
    }

    public int getPriority() {
        return priority;
    }

    public Msg setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public int getDeleteAfterProcessingTime() {
        return deleteAfterProcessingTime;
    }

    public Msg setDeleteAfterProcessingTime(int deleteAfterProcessingTime) {
        this.deleteAfterProcessingTime = deleteAfterProcessingTime;
        return this;
    }

    public boolean isDeleteAfterProcessing() {
        return deleteAfterProcessing;
    }

    public Msg setDeleteAfterProcessing(boolean deleteAfterProcessing) {
        this.deleteAfterProcessing = deleteAfterProcessing;
        return this;
    }

    public boolean isExclusive() {
        if (exclusive==null) return false;
        return exclusive.booleanValue();
    }

    /**
     * if true (default) message can only be processed by one system at a time
     *
     * @param exclusive
     */
    public Msg setExclusive(boolean exclusive) {
        // if (!exclusive) {
        //     lockedBy = "ALL";
        // } else {
        //     lockedBy = null;
        // }
        this.exclusive = exclusive;
        return this;
    }

    public String getSenderHost() {
        return senderHost;
    }

    public Msg setSenderHost(String senderHost) {
        this.senderHost = senderHost;
        return this;
    }

    @SuppressWarnings("unused")
    public Date getDeleteAt() {
        return deleteAt;
    }

    public Msg setDeleteAt(Date deleteAt) {
        this.deleteAt = deleteAt;
        return this;
    }

    public Msg addRecipient(String id) {
        if (recipients == null) {
            recipients = new ArrayList<>();
        }

        if (!recipients.contains(id)) {
            recipients.add(id);
        }

        return this;
    }

    @SuppressWarnings("unused")
    public Msg removeRecipient(String id) {
        if (recipients != null) {
            recipients.remove(id);
        }

        return this;
    }

    public Msg addValue(String key, Object value) {
        if (mapValue == null) {
            mapValue = new HashMap<>();
        }

        mapValue.put(key, value);
        return this;
    }

    @SuppressWarnings("unused")
    public Msg removeValue(String key) {
        if (mapValue == null) {
            return this;
        }

        mapValue.remove(key);
        return this;
    }

    @SuppressWarnings("unused")
    public Map<String, Object> getMapValue() {
        return mapValue;
    }

    public Msg setMapValue(Map<String, Object> mapValue) {
        this.mapValue = mapValue;
        return this;
    }

    public List<String> getTo() {
        return recipients;
    }

    public Msg setTo(List<String> to) {
        this.recipients = to;
        return this;
    }

    public MorphiumId getInAnswerTo() {
        return inAnswerTo;
    }

    public Msg setInAnswerTo(MorphiumId inAnswerTo) {
        this.inAnswerTo = inAnswerTo;
        return this;
    }

    public MorphiumId getMsgId() {
        return msgId;
    }

    public Msg setMsgId(MorphiumId msgId) {
        this.msgId = msgId;
        return this;
    }

    public String getName() {
        return name;
    }

    public Msg setName(String name) {
        this.name = name;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Msg setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public List<String> getProcessedBy() {
        if (processedBy == null) {
            processedBy = new ArrayList<>();
        }

        return processedBy;
    }

    public Msg setProcessedBy(List<String> processedBy) {
        this.processedBy = processedBy;
        return this;
    }

    public Msg addProcessedId(String id) {
        getProcessedBy().add(id);
        return this;
    }

    // public String getLockedBy() {
    //     return lockedBy;
    // }
    //
    // public Msg setLockedBy(String lockedBy) {
    //     this.lockedBy = lockedBy;
    //     return this;
    // }

    // @SuppressWarnings("unused")
    // public long getLocked() {
    //     return locked;
    // }
    //
    // public Msg setLocked(long locked) {
    //     this.locked = locked;
    //     return this;
    // }

    public String getSender() {
        return sender;
    }

    public Msg setSender(String sender) {
        this.sender = sender;
        return this;
    }

    public long getTtl() {
        return ttl;
    }

    public Msg setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public Msg setMsg(String msg) {
        this.msg = msg;
        return this;
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

    public Msg setValue(String value) {
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        return "Msg{" +
               " msgId='" + msgId + '\'' +
               ", inAnswerTo='" + inAnswerTo + '\'' +
               ", exclusive='"+exclusive+"'"+
               // ", lockedBy='" + lockedBy + '\'' +
               // ", locked=" + locked +
               ", ttl=" + ttl +
               ", sender='" + sender + '\'' +
               ", name='" + name + '\'' +
               ", msg='" + msg + '\'' +
               ", value='" + value + '\'' +
               ", timestamp=" + timestamp +
               ", additional='" + additional + '\'' +
               ", mapValue='" + mapValue + '\'' +
               ", recipient='" + recipients + '\'' +
               ", processedBy=" + processedBy +
               '}';
    }

    @PreStore
    public void preStore() {
        if (sender == null) {
            throw new RuntimeException("Cannot send msg anonymously - set Sender first!");
        }

        if (name == null) {
            throw new RuntimeException("Cannot send a message without name!");
        }

        if (timingOut) {
            if (ttl == 0) {
                LoggerFactory.getLogger(Msg.class).debug("Defaulting msg ttl to 30sec");
                ttl = 30000;
            }

            if (deleteAt == null) {
                deleteAt = new Date(System.currentTimeMillis() + ttl);
            }
        } else {
            deleteAt = null;
            ttl = 0;
        }

        if (!timingOut && !deleteAfterProcessing) {
            LoggerFactory.getLogger(Msg.class).warn("This message will never be deleted! this is not recommended!");
        }

        // if (getProcessedBy().size() == 0) {
        //     processedBy = null;
        // }

        timestamp = System.currentTimeMillis();
    }

    public boolean isAnswer() {
        return inAnswerTo != null;
    }

    public Msg createAnswerMsg() {
        Msg ret = new Msg(name, msg, value, ttl);
        ret.setInAnswerTo(msgId);
        ret.addRecipient(sender);
        ret.setPriority(priority);
        ret.setTimingOut(isTimingOut());
        ret.setDeleteAfterProcessingTime(deleteAfterProcessingTime);
        ret.setDeleteAfterProcessing(deleteAfterProcessing);
        return ret;
    }

    public void sendAnswer(Messaging messaging, Msg m) {
        m.setInAnswerTo(this.msgId);
        //m.addRecipient(this.getSender());
        m.addRecipient(this.getSender());
        m.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl()));
        m.setMsgId(new MorphiumId());
        messaging.sendMessage(m);
    }

    public Msg setRecipient(String id) {
        if (recipients == null) { recipients = new ArrayList<>(); }

        recipients.clear();
        recipients.add(id);
        return this;
    }

    public List<String> getRecipients() {
        return recipients;
    }

    public void setRecipients(List<String> recipients) {
        this.recipients = recipients;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }

        if (!(o instanceof Msg)) { return false; }

        Msg msg = (Msg) o;
        return Objects.equals(getMsgId(), msg.getMsgId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMsgId());
    }

    public enum Fields {msgId, exclusive,ttl, sender, senderHost, recipients, to, inAnswerTo, name, msg, additional, mapValue, value, timestamp, deleteAt, priority, processedBy}
}
