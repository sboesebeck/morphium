package de.caluga.morphium.gui.recordedit;

public class RecordChangedEvent {
    private Object theRecord;
    private boolean isNew;

    public RecordChangedEvent(Object r, boolean n) {
        isNew = n;
        theRecord = r;
    }

    public boolean isNew() {
        return isNew;
    }

    public Object getRecord() {
        return theRecord;
    }
}
