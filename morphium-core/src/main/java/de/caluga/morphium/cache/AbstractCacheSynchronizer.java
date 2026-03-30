package de.caluga.morphium.cache;

import de.caluga.morphium.Morphium;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.18
 * Time: 22:31
 *
 * @param <T> - the type of the cache sync listener
 */
@SuppressWarnings("unused")
public abstract class AbstractCacheSynchronizer<T extends CacheSyncListener> {
    /** Logger for this class. */
    protected static final Logger log = LoggerFactory.getLogger(MessagingCacheSynchronizer.class);
    /** The morphium instance this synchronizer operates on. */
    protected final Morphium morphium;
    /** List of all registered cache sync listeners. */
    protected final List<T> listeners = Collections.synchronizedList(new ArrayList<>());
    /** Map of type-specific listeners. */
    protected final Hashtable<Class<?>, Vector<T>> listenerForType = new Hashtable<>();


    /**
     * Creates a new AbstractCacheSynchronizer for the given morphium instance.
     * @param morphium the morphium instance
     */
    public AbstractCacheSynchronizer(Morphium morphium) {
        this.morphium = morphium;
    }

    /**
     * Adds a cache sync listener for all types.
     * @param cl the listener to add
     */
    public void addSyncListener(T cl) {
        listeners.add(cl);
    }

    /**
     * Removes a cache sync listener for all types.
     * @param cl the listener to remove
     */
    public void removeSyncListener(T cl) {
        listeners.remove(cl);
    }

    /**
     * Adds a cache sync listener for the specified type.
     * @param type the type
     * @param cl the listener to add
     */
    @SuppressWarnings("rawtypes")
    public void addSyncListener(Class type, T cl) {
        listenerForType.putIfAbsent(type, new Vector<>());
        listenerForType.get(type).add(cl);
    }

    /**
     * Removes a cache sync listener for the specified type.
     * @param type the type
     * @param cl the listener to remove
     */
    @SuppressWarnings("rawtypes")
    public void removeSyncListener(Class type, T cl) {
        if (listenerForType.get(type) == null) {
            return;
        }
        listenerForType.get(type).remove(cl);
    }

    /**
     * Fires a pre-clear event to all registered listeners for the given type.
     * @param type the type
     * @throws CacheSyncVetoException if sync is vetoed
     */
    @SuppressWarnings("rawtypes")
    protected void firePreClearEvent(Class type) throws CacheSyncVetoException {
        for (T cl : listeners) {
            cl.preClear(type);
        }
        if (type == null) {
            return;
        }
        if (listenerForType.get(type) != null) {
            for (T cl : listenerForType.get(type)) {
                cl.preClear(type);
            }
        }
    }

    /**
     * Fires a post-clear event to all registered listeners for the given type.
     * @param type the type
     */
    @SuppressWarnings("rawtypes")
    public void firePostClearEvent(Class type) {
        for (T cl : listeners) {
            cl.postClear(type);
        }
        if (type == null) {
            return;
        }
        if (listenerForType.get(type) != null) {
            for (T cl : listenerForType.get(type)) {
                cl.postClear(type);
            }
        }
    }
}
