package de.caluga.morphium.driver;/**
 * Created by stephan on 22.03.16.
 */

import java.util.List;
import java.util.Map;

/**
 * Morphiums representation of the mongodb Cursor.
 **/
public class MorphiumCursor<T> {
    private long cursorId;
    private List<Map<String, Object>> batch;
    private T internalCursorObject;

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

    public T getInternalCursorObject() {
        return internalCursorObject;
    }

    public void setInternalCursorObject(T internalCursorObject) {
        this.internalCursorObject = internalCursorObject;
    }
}
