package de.caluga.morphium.driver;/**
 * Created by stephan on 22.03.16.
 */

import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumCursor<T> {
    private long cursorId;
    private List<Map<String, Object>> result;
    private T internalCursorObject;

    public long getCursorId() {
        return cursorId;
    }

    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public List<Map<String, Object>> getResult() {
        return result;
    }

    public void setResult(List<Map<String, Object>> result) {
        this.result = result;
    }

    public T getInternalCursorObject() {
        return internalCursorObject;
    }

    public void setInternalCursorObject(T internalCursorObject) {
        this.internalCursorObject = internalCursorObject;
    }
}
