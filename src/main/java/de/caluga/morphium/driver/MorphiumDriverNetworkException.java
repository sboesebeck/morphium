package de.caluga.morphium.driver;/**
 * Created by stephan on 09.11.15.
 */

import java.util.Map;

/**
 * network related issues when accessing the database
 **/
public class MorphiumDriverNetworkException extends MorphiumDriverException {
    @SuppressWarnings("unused")
    public MorphiumDriverNetworkException(String msg, Throwable t, String collection, String db, Map<String, Object> query) {
        super(msg, t, collection, db, query);
    }

    public MorphiumDriverNetworkException(String message) {
        super(message);
    }

    public MorphiumDriverNetworkException(String message, Throwable cause) {
        super(message, cause);
    }
}
