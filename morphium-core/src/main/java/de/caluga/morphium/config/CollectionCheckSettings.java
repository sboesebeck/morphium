package de.caluga.morphium.config;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Embedded;

@Embedded
public class CollectionCheckSettings extends Settings {

    private IndexCheck indexCheck = IndexCheck.WARN_ON_STARTUP;
    private CappedCheck cappedCheck = CappedCheck.WARN_ON_STARTUP;
    public IndexCheck getIndexCheck() {
        return indexCheck;
    }
    public CollectionCheckSettings setIndexCheck(IndexCheck indexCheck) {
        this.indexCheck = indexCheck;
        return this;
    }
    public CappedCheck getCappedCheck() {
        return cappedCheck;
    }
    public CollectionCheckSettings setCappedCheck(CappedCheck cappedCheck) {
        this.cappedCheck = cappedCheck;
        return this;
    }
    public enum IndexCheck {
        NO_CHECK, WARN_ON_STARTUP, CREATE_ON_WRITE_NEW_COL, CREATE_ON_STARTUP,
    }

    public enum CappedCheck {
        NO_CHECK, WARN_ON_STARTUP, CREATE_ON_STARTUP, CREATE_ON_WRITE_NEW_COL, CONVERT_EXISTING_ON_STARTUP,
    }
}
