package de.caluga.morphium.messaging;

import java.util.Date;

import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.driver.MorphiumId;

@Entity
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
public class MsgLock {
    @Id
    private MorphiumId id;

    @Index(options = { "expireAfterSeconds:0" })
    private Date deleteAt;

    @Index
    private String lockId;

    public MsgLock(Msg m) {
        this.id = m.getMsgId();

        if (m.isTimingOut()) {
            this.deleteAt = new Date(System.currentTimeMillis() + m.getTtl());
        }
    }
    public MsgLock(MorphiumId id, Date deleteAt) {
        this.id = id;
        this.deleteAt = deleteAt;
    }

    public MsgLock(MorphiumId id) {
        this.id = id;
    }

    public String getLockId() {
        return lockId;
    }
    public void setLockId(String lockId) {
        this.lockId = lockId;
    }
    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public Date getDeleteAt() {
        return deleteAt;
    }

    public void setDeleteAt(Date deleteAt) {
        this.deleteAt = deleteAt;
    }

    public enum Fields {
        id,deleteAt,lockId,
    }

}
