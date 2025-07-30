package de.caluga.morphium.config;

import de.caluga.morphium.MorphiumConfig;

public class CollectionCheckSettings {

    private IndexCheck indexCheck = IndexCheck.WARN_ON_STARTUP;
    private CappedCheck cappedCheck = CappedCheck.WARN_ON_STARTUP;
    public IndexCheck getIndexCheck() {
        return indexCheck;
    }
    public void setIndexCheck(IndexCheck indexCheck) {
        this.indexCheck = indexCheck;
    }
    public CappedCheck getCappedCheck() {
        return cappedCheck;
    }
    public void setCappedCheck(CappedCheck cappedCheck) {
        this.cappedCheck = cappedCheck;
    }
    public enum IndexCheck {
        NO_CHECK, WARN_ON_STARTUP, CREATE_ON_WRITE_NEW_COL, CREATE_ON_STARTUP,
    }

    public enum CappedCheck {
        NO_CHECK, WARN_ON_STARTUP, CREATE_ON_STARTUP, CREATE_ON_WRITE_NEW_COL, CONVERT_EXISTING_ON_STARTUP,
    }
}
