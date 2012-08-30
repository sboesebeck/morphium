package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

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
    private ObjectId id;
    @Index
    private String name;
    private Long currentValue;
    @Index
    private String lockedBy;


    /**
     * Property name constant for {@code lockedBy}.
     */
    public static final String PROPERTYNAME_LOCKED_BY = "lockedBy";
    /**
     * Property name constant for {@code name}.
     */
    public static final String PROPERTYNAME_NAME = "name";
    /**
     * Property name constant for {@code currentValue}.
     */
    public static final String PROPERTYNAME_CURRENT_VALUE = "currentValue";

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

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
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
