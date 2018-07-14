package de.caluga.morphium.cache;

import de.caluga.morphium.Morphium;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.18
 * Time: 22:31
 * <p>
 * TODO: Add documentation here
 */
public abstract class AbstractCacheSynchronizer<T extends CacheSyncListener> {
    protected static final Logger log = LoggerFactory.getLogger(MessagingCacheSynchronizer.class);
    protected Morphium morphium;
    protected List<T> listeners = Collections.synchronizedList(new ArrayList<>());
    protected Hashtable<Class<?>, Vector<T>> listenerForType = new Hashtable<>();


    public AbstractCacheSynchronizer(Morphium morphium) {
        this.morphium = morphium;
    }

    public void addSyncListener(T cl) {
        listeners.add(cl);
    }

    public void removeSyncListener(T cl) {
        listeners.remove(cl);
    }

    public void addSyncListener(Class type, T cl) {
        listenerForType.putIfAbsent(type, new Vector<>());
        listenerForType.get(type).add(cl);
    }

    public void removeSyncListener(Class type, T cl) {
        if (listenerForType.get(type) == null) {
            return;
        }
        listenerForType.get(type).remove(cl);
    }

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
