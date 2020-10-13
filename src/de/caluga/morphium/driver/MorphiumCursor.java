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
    private List<Map<String, Object>> batch;
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

    public List<Map<String, Object>> getBatch() {
        return batch;
    }

    public void setBatch(List<Map<String, Object>> batch) {
        this.batch = batch;
    }

    public Object getInternalCursorObject() {
        return internalCursorObject;
    }

    public void setInternalCursorObject(Object internalCursorObject) {
        this.internalCursorObject = internalCursorObject;
    }
}
