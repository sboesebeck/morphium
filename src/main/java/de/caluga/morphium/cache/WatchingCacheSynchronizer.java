package de.caluga.morphium.cache;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;

import java.util.Set;

@SuppressWarnings("EmptyMethod")
public class WatchingCacheSynchronizer extends AbstractCacheSynchronizer<WatchingCacheSyncListener> implements ChangeStreamListener {

    private boolean running = true;
    private final ChangeStreamMonitor monitor;

    public WatchingCacheSynchronizer(Morphium m) {
        super(m);

        monitor = new ChangeStreamMonitor(m);
        monitor.addListener(this);
        monitor.start();
    }

    public void start() {
        //well not really necessary
    }

    public void terminate() {
        running = false;
        monitor.terminate();
    }

    public boolean isActive() {
        return monitor.isRunning();
    }

    @Override
    public boolean incomingData(ChangeStreamEvent evt) {
        if (evt.getOperationType().equals("invalidate")) {
            return running;
        }

        Set<Class<?>> types = morphium.getCache().getCachedTypes();
        for (Class<?> t : types) {
            String s = morphium.getMapper().getCollectionName(t);
            if (s.equals(evt.getCollectionName())) {
//                log.info("Incoming changestream event");

                Cache cache = morphium.getARHelper().getAnnotationFromHierarchy(t, Cache.class);
                if (cache != null) {
                    Object id = evt.getDocumentKey();
                    try {
                        firePreClearEvent(t);
                    } catch (CacheSyncVetoException e) {
                        log.warn("Cache clear aborted due to Veto from Listener!");
                        break;
                    }
                    switch (cache.syncCache()) {
                        case CLEAR_TYPE_CACHE:
                            morphium.getCache().clearCachefor(t);
                            break;
                        case REMOVE_ENTRY_FROM_TYPE_CACHE:
                            morphium.getCache().removeEntryFromCache(t, id);
                            break;
                        case UPDATE_ENTRY:
                            Object ent = morphium.getCache().getFromIDCache(t, id);
                            morphium.reread(ent);
                            break;
                        case NONE:
                        default:
                            break;

                    }
                }
                firePostClearEvent(t);
                break;
            } else {
               log.info("Changestream event wrong name {} != {}",s,evt.getCollectionName());
            }
        }
        return running;
    }
}
