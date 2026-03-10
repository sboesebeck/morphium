package de.caluga.morphium.cache;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.18
 * Time: 22:39
 * <p>
 */
public interface CacheSyncListener {
    /**
     * before clearing cache - if cls == null whole cache
     * Message m contains information about reason and stuff...
     */
    @SuppressWarnings("UnusedParameters")
    void preClear(Class cls) throws CacheSyncVetoException;

    @SuppressWarnings("UnusedParameters")
    void postClear(Class cls);
}
