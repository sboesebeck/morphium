package de.caluga.morphium;

import java.util.Date;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;


/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:49
 * <p>
 * Sequence: Used by SequenceGenerator to crate unique sequential numbers. Locking and such is done by the Generator
 * ReadPreference: MasterOnly and SavetyLevel=WAIT_FOR_SLAVE means: read only from master, but wait for one slave to
 * commit the write. This is best compromise between performance and security (as usually you'd call nextValue on the
 * generator, not getcurrentvalue all the time.
 */
@Entity(typeId = "sequence")
@NoCache
@WriteSafety(timeout = 10000, level = SafetyLevel.WAIT_FOR_SLAVE)
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
public class Sequence {
    @Id
    private String name;
    private Long currentValue;


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


    @Override
    public String toString() {
        return "Sequence{" +
                "name='" + name + '\'' +
                ", currentValue=" + currentValue +
                '}';
    }


    public enum Fields {name, currentValue}

    @Entity(typeId = "seq_lock")
    public static class SeqLock{
        @Id
        private String name;
        @Index
        private String lockedBy;

        @Index(options = {"expireAfterSeconds:30"})
        private Date lockedAt;
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public String getLockedBy() {
            return lockedBy;
        }
        public void setLockedBy(String lockedBy) {
            this.lockedBy = lockedBy;
        }
        public Date getLockedAt() {
            return lockedAt;
        }
        public void setLockedAt(Date lockedAt) {
            this.lockedAt = lockedAt;
        }


    }
}
