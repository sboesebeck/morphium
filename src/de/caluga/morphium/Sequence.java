package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:49
 * <p/>
 */
@Entity
@NoCache
@Index({"name,locked_by"})
@WriteSafety(waitForJournalCommit = true, waitForSync = true, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
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
}
