package de.caluga.morphium.messaging;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PreStore;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:45
 * <p/>
 * TODO: Add documentation here
 */
@Entity
@Cache(readCache = false, writeCache = false, clearOnWrite = false)
//MAximumSecurity
@WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVES, timeout = 3000, waitForJournalCommit = true, waitForSync = true)
@Lifecycle
public class Msg {
    public static enum Fields {
        lstOfIdsAlreadyProcessed,
        lockedBy,
        msgId,
        locked,
        type,
        inAnswerTo,
        to,
        msg,
        additional,
        value,
        timestamp,
        sender,
        ttl
    }

    @Id
    private ObjectId id;
    private List<String> lstOfIdsAlreadyProcessed;
    private String msgId;
    private String lockedBy;
    private long locked;
    private MsgType type;
    private long ttl;
    private String sender;
    private List<String> to;
    private String inAnswerTo;
    //payload goes here
    private String name;
    private String msg;
    private List<String> additional;
    private Map<String,Object> mapValue;
    private String value;

    private long timestamp;

    public Msg() {
        msgId= UUID.randomUUID().toString();
    }

    public Msg( String name, String msg, String value) {
        this( name, MsgType.SINGLE, msg,  value, 30000);
    }

    public Msg(String name, MsgType t, String msg, String value, long ttl) {
        this();
        this.name = name;
        this.msg = msg;
        this.value = value;
        this.type = t;
        this.ttl = ttl;
    }

    public void addRecipient(String id) {
        if (to==null) {
            to=new ArrayList<String>();

        }
        if (!to.contains(id)) {
            to.add(id);
        }
    }

    public void removeRecipient(String id) {
        if (to==null) {
            return;

        } else {
            to.remove(id);
        }
    }

    public void addValue(String key, Object value) {
        if (mapValue==null) {
            mapValue=new HashMap<String, Object>();
        }
        mapValue.put(key,value);
    }

    public void removeValue(String key) {
        if (mapValue==null) return;
        mapValue.remove(key);
    }

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

    public String getInAnswerTo() {
        return inAnswerTo;
    }

    public void setInAnswerTo(String inAnswerTo) {
        this.inAnswerTo = inAnswerTo;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
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

    public List<String> getLstOfIdsAlreadyProcessed() {
        return lstOfIdsAlreadyProcessed;
    }

    public void setLstOfIdsAlreadyProcessed(List<String> lstOfIdsAlreadyProcessed) {
        this.lstOfIdsAlreadyProcessed = lstOfIdsAlreadyProcessed;
    }

    public void addProcessedId(String id) {
        if (lstOfIdsAlreadyProcessed == null) {
            lstOfIdsAlreadyProcessed = new ArrayList<String>();
        }
        lstOfIdsAlreadyProcessed.add(id);
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

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

    public MsgType getType() {
        return type;
    }

    public void setType(MsgType type) {
        this.type = type;
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

    public List<String> getAdditional() {
        return additional;
    }

    public void setAdditional(List<String> additional) {
        this.additional = additional;
    }

    public void addAdditional(String value) {
        if (additional==null) {
            additional=new ArrayList<String>();
        }
        additional.add(value);
    }

    public void removeAdditional(String value) {
        if (additional==null) return;
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
                "id=" + id +
                ", inAnswerTo='" + inAnswerTo + '\'' +
                ", msgId='" + msgId + '\'' +
                ", lockedBy='" + lockedBy + '\'' +
                ", locked=" + locked +
                ", type=" + type +
                ", ttl=" + ttl +
                ", sender='" + sender + '\'' +
                ", name='" + name + '\'' +
                ", msg='" + msg + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", additional='" + additional + '\'' +
                ", mapValue='" + mapValue + '\'' +
                ", recipients='" + to + '\'' +
                ", lstOfIdsAlreadyProcessed=" + lstOfIdsAlreadyProcessed +
                '}';
    }

    @PreStore
    public void preStore() {
        if (sender == null) {
            throw new RuntimeException("Cannot send msg anonymously - set Sender first!");
        }
        if (type == null) {
            Logger.getLogger(Msg.class).warn("Messagetype not set - using SINGLE");
            type = MsgType.SINGLE;
        }
        if (name == null) {
            throw new RuntimeException("Cannot send a message without name!");
        }
        if (ttl == 0) {
            Logger.getLogger(Msg.class).warn("Defaulting msg ttl to 30sec");
            ttl = 30000;
        }
        timestamp = System.currentTimeMillis();
    }


    public Msg createAnswerMsg() {
        Msg ret=new Msg(name,type, msg, value,ttl);
        ret.setInAnswerTo(msgId);
        ret.addRecipient(sender);
        return ret;
    }
    public void sendAnswer(Messaging messaging, Msg m) {
        m.setInAnswerTo(this.msgId);
        m.addRecipient(this.getSender());
        messaging.queueMessage(m);
    }
}
