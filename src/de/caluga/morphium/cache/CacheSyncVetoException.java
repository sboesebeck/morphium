package de.caluga.morphium.cache;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.12
 * Time: 17:27
 * <p/>
 */
public class CacheSyncVetoException extends Exception {
    public CacheSyncVetoException() {
        super();
    }

    public CacheSyncVetoException(String message) {
        super(message);
    }

    public CacheSyncVetoException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheSyncVetoException(Throwable cause) {
        super(cause);
    }
}
