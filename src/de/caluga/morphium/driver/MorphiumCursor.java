package de.caluga.morphium.driver;/**
 * Created by stephan on 22.03.16.
 */

import java.util.List;
import java.util.Map;

/**
 * Morphiums representation of the mongodb Cursor.
 **/
public abstract class MorphiumCursor {
    private long cursorId;
    private int batchSize;
    private List<Map<String, Object>> batch;
    private String db;
    private String collection;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

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

    public abstract boolean hasNext() throws MorphiumDriverException;

    public abstract Map<String, Object> next() throws MorphiumDriverException;

    public abstract void close() throws MorphiumDriverException;

    public abstract int available() throws MorphiumDriverException;

}
