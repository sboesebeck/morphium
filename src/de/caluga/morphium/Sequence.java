package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.NoCache;

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:49
 * <p/>
 */
@Entity
@NoCache
@Index
@WriteSafety(waitForJournalCommit = true, waitForSync = true, level = SafetyLevel.WAIT_FOR_SLAVES)
public class Sequence {
    private String name;
    private Long currentValue;
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
