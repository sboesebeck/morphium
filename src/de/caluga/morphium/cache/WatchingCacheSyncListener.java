package de.caluga.morphium.cache;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.18
 * Time: 22:44
 * <p>
 * TODO: Add documentation here
 */
public interface WatchingCacheSyncListener extends CacheSyncListener {
    void preClear(Class<?> type, String operation);

}
