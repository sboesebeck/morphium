package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgType;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.Hashtable;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 27.05.12
 * Time: 14:14
 * <p/>
 * Connects to the Messaging system and to morphium. As soon as there is a message coming with name "cacheSync", this
 * is responsible for clearing the local cache for this entity.
 * Messaging used:
 * <ul>
 * <li> Msg.name == Always cacheSync</li>
 * <li> Msg.type == Always multi - more than one node may listen to those messages</li>
 * <li> Msg.msg == The messag is the Reason for clearing (delete, store, user interaction...)</li>
 * <li> Msg.value == String, name of the class whose cache should be cleared</li>
 * <li> Msg.additional == always null </li>
 * <li> Msg.ttl == 30 sec - shoule be enought time for the message to be processed by all nodes</li>
 * </ul>
 */
@SuppressWarnings("UnusedDeclaration")
public class CacheSynchronizer extends MorphiumStorageAdapter<Object> implements MessageListener {
    private static final Logger log = Logger.getLogger(CacheSynchronizer.class);

    private Messaging messaging;
    private Morphium morphium;

    public static final String CACHE_SYNC_TYPE = "cacheSyncType";
    public static final String CACHE_SYNC_RECORD = "cacheSyncRecord";

    private Vector<CacheSyncListener> listeners = new Vector<CacheSyncListener>();
    private Hashtable<Class<?>, Vector<CacheSyncListener>> listenerForType = new Hashtable<Class<?>, Vector<CacheSyncListener>>();
    private boolean attached;
    private AnnotationAndReflectionHelper annotationHelper;

    /**
     * @param msg      - primary messaging, will attach to and send messages over
     * @param morphium - the underlying morphium instance
     */
    public CacheSynchronizer(Messaging msg, Morphium morphium) {
        messaging = msg;
        this.morphium = morphium;
        annotationHelper = morphium.getARHelper();
        morphium.addListener(this);

        messaging.addListenerForMessageNamed(CACHE_SYNC_TYPE, this);
        messaging.addListenerForMessageNamed(CACHE_SYNC_RECORD, this);
        attached = true;

    }

    public void addSyncListener(CacheSyncListener cl) {
        listeners.add(cl);
    }

    public void removeSyncListener(CacheSyncListener cl) {
        listeners.remove(cl);
    }

    public void addSyncListener(Class type, CacheSyncListener cl) {
        if (listenerForType.get(type) == null) {
            listenerForType.put(type, new Vector<CacheSyncListener>());
        }
        listenerForType.get(type).add(cl);
    }

    public void removeSyncListener(Class type, CacheSyncListener cl) {
        if (listenerForType.get(type) == null) {
            return;
        }
        listenerForType.get(type).remove(cl);
    }


    public void firePostSendEvent(Class type, Msg m) {
        for (CacheSyncListener cl : listeners) {
            cl.postSendClearMsg(type, m);
        }
        if (type == null) return;
        if (listenerForType.get(type) != null) {
            for (CacheSyncListener cl : listenerForType.get(type)) {
                cl.postSendClearMsg(type, m);
            }
        }
    }

    public void firePreSendEvent(Class type, Msg m) throws CacheSyncVetoException {
        for (CacheSyncListener cl : listeners) {
            cl.preSendClearMsg(type, m);
        }
        if (type == null) return;
        if (listenerForType.get(type) != null) {
            for (CacheSyncListener cl : listenerForType.get(type)) {
                cl.preSendClearMsg(type, m);
            }
        }
    }

    private void firePreClearEvent(Class type, Msg m) throws CacheSyncVetoException {
        for (CacheSyncListener cl : listeners) {
            cl.preClear(type, m);
        }
        if (type == null) return;
        if (listenerForType.get(type) != null) {
            for (CacheSyncListener cl : listenerForType.get(type)) {
                cl.preClear(type, m);
            }
        }
    }

    public void firePostClearEvent(Class type, Msg m) {
        for (CacheSyncListener cl : listeners) {
            cl.postClear(type, m);
        }
        if (type == null) return;
        if (listenerForType.get(type) != null) {
            for (CacheSyncListener cl : listenerForType.get(type)) {
                cl.postClear(type, m);
            }
        }
    }

    public void sendClearMessage(Object record, String reason, boolean isNew) {
//        long start = System.currentTimeMillis();
        if (record.equals(Msg.class)) return;
        Object id = morphium.getId(record);

        Msg m = new Msg(CACHE_SYNC_RECORD, MsgType.MULTI, reason, record.getClass().getName(), 30000);
        if (id != null)
            m.addAdditional(id.toString());
        Cache c = annotationHelper.getAnnotationFromHierarchy(record.getClass(), Cache.class); //(Cache) type.getAnnotation(Cache.class);
        if (c == null) return; //not clearing cache for non-cached objects
        if (c.readCache() && c.clearOnWrite()) {
            if (c.syncCache().equals(Cache.SyncCacheStrategy.UPDATE_ENTRY) || c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
                if (!isNew) {
                    try {
                        firePreSendEvent(record.getClass(), m);
                        messaging.queueMessage(m);
                        firePostSendEvent(record.getClass(), m);
                    } catch (CacheSyncVetoException e) {
                        log.error("could not send clear cache message: Veto by listener!", e);
                    }

                }
            } else if (c.syncCache().equals(Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)) {
                m.setName(CACHE_SYNC_TYPE);
                try {
                    firePreSendEvent(record.getClass(), m);
                    messaging.queueMessage(m);
                    firePostSendEvent(record.getClass(), m);
                } catch (CacheSyncVetoException e) {
                    log.error("could not send clear cache message: Veto by listener!", e);
                }
            }
        }
//        long dur = System.currentTimeMillis() - start;
//        log.info("Queueing cache sync message took "+dur+" ms");
    }

    public void sendClearMessage(Class type, String reason) {
        sendClearMessage(type, reason, false);
    }

    /**
     * sends message if necessary
     *
     * @param type   - type
     * @param reason - reason
     */
    public void sendClearMessage(Class type, String reason, boolean force) {
        if (type.equals(Msg.class)) return;
        Msg m = new Msg(CACHE_SYNC_TYPE, MsgType.MULTI, reason, type.getName(), 30000);
        Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class); //(Cache) type.getAnnotation(Cache.class);
        if (c == null) return; //not clearing cache for non-cached objects
        if ((c.readCache() && c.clearOnWrite() && !c.syncCache().equals(Cache.SyncCacheStrategy.NONE)) || force) {
            try {
                firePreSendEvent(type, m);
                messaging.queueMessage(m);
                firePostSendEvent(type, m);
            } catch (CacheSyncVetoException e) {
                log.error("could not send clear message: Veto!", e);
            }
        }
    }

    public void detach() {
        attached = false;
        morphium.removeListener(this);
        messaging.removeListenerForMessageNamed(CACHE_SYNC_TYPE, this);
        messaging.removeListenerForMessageNamed(CACHE_SYNC_RECORD, this);
    }

    public boolean isAttached() {
        return attached;
    }

    public void sendClearAllMessage(String reason) {
        Msg m = new Msg(CACHE_SYNC_TYPE, MsgType.MULTI, reason, "ALL", 30000);
        try {
            firePreSendEvent(null, m);
            messaging.queueMessage(m);
            firePostSendEvent(null, m);
        } catch (CacheSyncVetoException e) {
            log.error("Got veto before clearing cache", e);
        }
    }


    @Override
    public void postStore(Morphium m, Object r, boolean isNew) {
        sendClearMessage(r, "store", isNew);
    }


    @Override
    public void postDrop(Morphium m, Class cls) {
        sendClearMessage(cls, "drop");
    }

    @Override
    public void preDrop(Morphium m, Class cls) {
    }

//    @Override
//    public void postListStore(List<Object> lst, Map<Object, Boolean> isNew) {
////        List<Class> toClear = new ArrayList<Class>();
////        for (Object o : lst) {
////            if (!toClear.contains(o.getClass())) {
////                toClear.add(o.getClass());
////            }
////        }
////        for (Class c : toClear) {
////            sendClearMessage(c, "store");
////        }
//        sendClearMessage(lst, isNew,"list store");
//    }
//
//    @Override
//    public void preListStore(List<Object> lst, Map<Object,Boolean> isNew) {
//    }


    @Override
    public void postRemove(Morphium m, Query q) {
        sendClearMessage(q.getType(), "remove");
    }


    @Override
    public void postUpdate(Morphium m, Class cls, Enum updateType) {
        sendClearMessage(cls, "Update: " + updateType.name());
    }

    @Override
    public Msg onMessage(Messaging msg, Msg m) {
        Msg answer = new Msg("clearCacheAnswer", "processed", messaging.getSenderId());
        try {
            if (log.isDebugEnabled()) {
                String action = m.getMsg();
                String sender = m.getSender();
                log.debug("Got message " + m.getName() + " from " + sender + " - Action: " + action + " Class: " + m.getValue());
            }
            if (m.getName().equals(CACHE_SYNC_TYPE)) {
                if (m.getValue().equals("ALL")) {

                    try {
                        firePreClearEvent(null, m);
                        morphium.getCache().resetCache();
                        firePostClearEvent(null, m);
                        answer.setMsg("cache completely cleared");
                        log.info("Cache completely cleared");
                    } catch (CacheSyncVetoException e) {
                        log.error("Could not clear whole cache - Veto!", e);
                    }
                    return answer;
                }
                Class cls = Class.forName(m.getValue());
                if (annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                    Cache c = annotationHelper.getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
                    if (c != null) {
                        if (c.readCache()) {
                            try {
                                //Really clearing cache, even if clear on write is set to false! => manual clearing?
                                firePreClearEvent(cls, m);
                                morphium.clearCachefor(cls);
                                answer.setMsg("cache cleared for type: " + m.getValue());
                                firePostClearEvent(cls, m);
                            } catch (CacheSyncVetoException e) {
                                log.error("Could not clear cache! Got Veto", e);
                            }
                        } else {
                            log.warn("trying to clear cache for uncached enitity or one where clearOnWrite is false");
                            answer.setMsg("type is uncached or clearOnWrite is false: " + m.getValue());
                        }
                    }
                } else {
                    log.warn("Trying to clear cache for none-Entity?????");
                    answer.setMsg("cannot clear cache for non-entyty type: " + m.getValue());

                }
            } else {
                //must be CACHE_SYNC_RECORD
                Class cls = Class.forName(m.getValue());
                if (annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                    Cache c = annotationHelper.getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
                    if (c != null) {
                        if (c.readCache()) {
                            try {
                                firePreClearEvent(cls, m);
                                Hashtable<Class<?>, Hashtable<Object, Object>> idCache = morphium.getCache().cloneIdCache();
                                for (String id : m.getAdditional()) {
                                    if (idCache.get(cls) != null) {
                                        Object toUpdate = idCache.get(cls).get(id);
                                        if (toUpdate == null && (id instanceof String)) {
                                            //Try objectId
                                            toUpdate = idCache.get(cls).get(new ObjectId(id));
                                        }
                                        if (toUpdate != null) {
                                            //Object is updated in place!
                                            if (c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
                                                morphium.getCache().removeEntryFromCache(cls, id);
                                            } else {
                                                morphium.reread(idCache.get(cls).get(id));

                                            }
                                        }
                                    }
                                }
                                morphium.getCache().setIdCache(idCache);
                                answer.setMsg("cache cleared for type: " + m.getValue());
                                firePostClearEvent(cls, m);
                            } catch (CacheSyncVetoException e) {
                                log.error("Not clearing id cache: Veto", e);
                            }

                        } else {
                            log.warn("trying to clear cache for uncached enitity or one where clearOnWrite is false");
                            answer.setMsg("type is uncached or clearOnWrite is false: " + m.getValue());
                        }
                    }
                }

            }
        } catch (ClassNotFoundException e) {
            log.warn("Could not process message for class " + m.getValue() + " - not found!");
            answer.setMsg("class not found: " + m.getValue());
        } catch (Throwable t) {
            log.error("Could not process message: ", t);
            answer.setMsg("Error processing message: " + t.getMessage());
        }
        return answer;
    }


}
