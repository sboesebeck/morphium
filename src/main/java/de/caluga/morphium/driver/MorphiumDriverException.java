package de.caluga.morphium.driver;/**
 * Created by stephan on 09.11.15.
 */

import java.util.Map;

/**
 * Base exception for all errors that occur while accessing the database through
 * the Morphium driver layer.
 * <p>
 * This is an <b>unchecked</b> exception ({@code RuntimeException}) — consistent
 * with the convention established by the official MongoDB Java driver
 * ({@code com.mongodb.MongoException extends RuntimeException}) and with the
 * general trend in modern Java persistence frameworks (JPA, jOOQ, Spring Data)
 * that treat database errors as unrecoverable at the point of call.
 * <p>
 * Making this unchecked eliminated the pervasive
 * {@code catch (MorphiumDriverException e) { throw new RuntimeException(e); }}
 * pattern that previously existed throughout the codebase wherever a
 * method could not declare checked exceptions — for example in
 * {@link java.util.Iterator} implementations, {@code MorphiumWriter} methods,
 * and lambda callbacks. Those catch-and-wrap blocks have been removed.
 *
 * @see MorphiumDriverNetworkException
 * @see FunctionNotSupportedException
 **/
public class MorphiumDriverException extends RuntimeException {
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

    @SuppressWarnings("unused")
    public Object getMongoReason() {
        return mongoReason;
    }

    public void setMongoReason(Object mongoReason) {
        this.mongoReason = mongoReason;
    }

    @SuppressWarnings("unused")
    public String getCollection() {
        return collection;
    }

    @SuppressWarnings("unused")
    public void setCollection(String collection) {
        this.collection = collection;
    }

    @SuppressWarnings("unused")
    public String getDb() {
        return db;
    }

    @SuppressWarnings("unused")
    public void setDb(String db) {
        this.db = db;
    }

    @SuppressWarnings("unused")
    public Map<String, Object> getQuery() {
        return query;
    }

    @SuppressWarnings("unused")
    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }
}
