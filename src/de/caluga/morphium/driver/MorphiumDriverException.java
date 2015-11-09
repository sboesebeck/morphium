package de.caluga.morphium.driver;/**
 * Created by stephan on 09.11.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumDriverException extends Exception {
    private String collection;
    private String db;
    private Map<String, Object> query;

    public MorphiumDriverException(String message, Throwable cause) {
        super(message, cause);
    }

    public MorphiumDriverException(String message, Throwable cause, String collection, String db, Map<String, Object> q) {
        super(message, cause);
        this.collection = collection;
        this.db = db;
        this.query = q;
    }


}
