package de.caluga.morphium.driver;/**
 * Created by stephan on 09.11.15.
 */

import java.util.Map;

/**
 * error during accessing the database through the driver
 **/
public class MorphiumDriverException extends Exception {
    private String collection;
    private String db;
    private Map<String, Object> query;
    private Object mongoCode;
    private Object mongoReason;

    public MorphiumDriverException(String message) {
        super(message);
    }

    public MorphiumDriverException(String message, Throwable cause) {
        super(message, cause);
    }

    public MorphiumDriverException(String message, Throwable cause, String collection, String db, Map<String, Object> q) {
        super(message, cause);
        this.collection = collection;
        this.db = db;
        this.query = q;
    }

    public Object getMongoCode() {
        return mongoCode;
    }

    public void setMongoCode(Object mongoCode) {
        this.mongoCode = mongoCode;
    }

    public Object getMongoReason() {
        return mongoReason;
    }

    public void setMongoReason(Object mongoReason) {
        this.mongoReason = mongoReason;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }
}
