package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.bson.MorphiumId;


/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:49
 * <p/>
 * Sequence: Used by SequenceGenerator to crate unique sequential numbers. Locking and such is done by the Generator
 * ReadPreference: MasterOnly and SavetyLevel=WAIT_FOR_SLAVE means: read only from master, but wait for one slave to
 * commit the write. This is best compromise between performance and security (as usually you'd call nextValue on the
 * generator, not getcurrentvalue all the time.
 */
@Entity
@NoCache
@Index({"name,locked_by"})
@WriteSafety(waitForJournalCommit = true, waitForSync = true, timeout = 10000, level = SafetyLevel.WAIT_FOR_SLAVE)
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
public class Sequence {
    @Id
    private MorphiumId id;
    @Index
    private String name;
    private Long currentValue;
    @Index
    private String lockedBy;
    private long lockedAt;


    public long getLockedAt() {
        return lockedAt;
    }

    @SuppressWarnings("unused")
    public void setLockedAt(long lockedAt) {
        this.lockedAt = lockedAt;
    }

    @SuppressWarnings("unused")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(Long currentValue) {
        this.currentValue = currentValue;
    }

    @SuppressWarnings("unused")
    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    @SuppressWarnings("unused")
    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Sequence{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", currentValue=" + currentValue +
                ", lockedBy='" + lockedBy + '\'' +
                '}';
    }
}
