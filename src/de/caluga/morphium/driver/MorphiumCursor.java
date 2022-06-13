package de.caluga.morphium.driver;/**
 * Created by stephan on 22.03.16.
 */

import java.util.List;
import java.util.Map;

/**
 * Morphiums representation of the mongodb Cursor.
 **/
public class MorphiumCursor {
    private long cursorId;
    private int batchSize;
    private List<Doc> batch;
    private Object internalCursorObject;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getCursorId() {
        return cursorId;
    }

    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public List<Doc> getBatch() {
        return batch;
    }

    public void setBatch(List<Doc> batch) {
        this.batch = batch;
    }

    public Object getInternalCursorObject() {
        return internalCursorObject;
    }

    public void setInternalCursorObject(Object internalCursorObject) {
        this.internalCursorObject = internalCursorObject;
    }
}
