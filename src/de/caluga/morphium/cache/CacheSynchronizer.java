package de.caluga.morphium.cache;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgType;
import de.caluga.morphium.query.Query;

import java.util.*;

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
 * <li> Msg.msg == The messag is the Reason for clearing (remove, store, user interaction...)</li>
 * <li> Msg.value == String, name of the class whose cache should be cleared</li>
 * <li> Msg.additional == always null </li>
 * <li> Msg.ttl == 30 sec - shoule be enought time for the message to be processed by all nodes</li>
 * </ul>
 */
@SuppressWarnings("UnusedDeclaration")
public class CacheSynchronizer implements MessageListener, MorphiumStorageListener<Object> {
    private static final Logger log = new Logger(CacheSynchronizer.class);

    private Messaging messaging;
    private Morphium morphium;

    public static final String CACHE_SYNC_TYPE = "cacheSyncType";
    public static final String CACHE_SYNC_RECORD = "cacheSyncRecord";

    private Vector<CacheSyncListener> listeners = new Vector<CacheSyncListener>();
    private Hashtable<Class<?>, Vector<CacheSyncListener>> listenerForType = new Hashtable<Class<?>, Vector<CacheSyncListener>>();
    private boolean attached;
    private AnnotationAndReflectionHelper annotationHelper;

    private boolean commitMessage = false;


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

    public void sendClearMessage(String reason, Map<Object, Boolean> isNew) {
//        long start = System.currentTimeMillis();


        Map<Class<?>, Map<Boolean, List<Object>>> sorted = new HashMap<Class<?>, Map<Boolean, List<Object>>>();

        for (Object record : isNew.keySet()) {
            Cache c = annotationHelper.getAnnotationFromHierarchy(record.getClass(), Cache.class); //(Cache) type.getAnnotation(Cache.class);
            if (c == null) continue; //not clearing cache for non-cached objects
            if (c.readCache() && c.clearOnWrite()) {
                if (sorted.get(record.getClass()) == null) {
                    sorted.put(record.getClass(), new HashMap<Boolean, List<Object>>());
                    sorted.get(record.getClass()).put(true, new ArrayList<Object>());
                    sorted.get(record.getClass()).put(false, new ArrayList<Object>());
                }
                sorted.get(record.getClass()).get(isNew.get(record)).add(record);

            }
        }

        for (Class<?> cls : sorted.keySet()) {
            Cache c = annotationHelper.getAnnotationFromHierarchy(cls, Cache.class); //(Cache) type.getAnnotation(Cache.class);

            ArrayList<Object> toUpdate = new ArrayList<Object>();
            ArrayList<Object> toClrCachee = new ArrayList<Object>();

            //not new objects
            for (Object record : sorted.get(cls).get(false)) {
                if (c.syncCache().equals(Cache.SyncCacheStrategy.UPDATE_ENTRY) || c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
                    toUpdate.add(record);

                } else if (c.syncCache().equals(Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)) {
                    toClrCachee.add(record);
                }
            }
            //new objects
            for (Object record : sorted.get(cls).get(true)) {

//                if (c.syncCache().equals(Cache.SyncCacheStrategy.UPDATE_ENTRY) || c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
//
//                } else
                if (c.syncCache().equals(Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)) { //cannot be updated, it's new
                    toClrCachee.add(record);
                }
            }

            if (toUpdate.size() != 0) {
                Msg m = new Msg(CACHE_SYNC_RECORD, MsgType.MULTI, reason, cls.getName(), 30000);

                for (Object k : toUpdate) {
                    if (!k.getClass().equals(Msg.class)) {
                        Object id = morphium.getId(k);
                        if (id != null) {
                            m.addAdditional(id.toString());
                        }
                    }
                }
                try {
                    firePreSendEvent(cls, m);
                    messaging.queueMessage(m);
                    firePostSendEvent(cls, m);
                } catch (CacheSyncVetoException e) {
                    log.warn("could not send clear cache message: Veto by listener!", e);
                }
            }

            if (toClrCachee.size() != 0) {
                Msg m = new Msg(CACHE_SYNC_TYPE, MsgType.MULTI, reason, cls.getName(), 30000);

                for (Object k : toUpdate) {
                    if (!k.getClass().equals(Msg.class)) {
                        Object id = morphium.getId(k);
                        if (id != null) {
                            m.addAdditional(id.toString());
                        }
                    }
                }
                try {
                    firePreSendEvent(cls, m);
                    messaging.queueMessage(m);
                    firePostSendEvent(cls, m);
                } catch (CacheSyncVetoException e) {
                    log.warn("could not send clear cache message: Veto by listener!", e);
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
    public void preStore(Morphium m, Object r, boolean isNew) throws MorphiumAccessVetoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void preStore(Morphium m, Map<Object, Boolean> isNew) throws MorphiumAccessVetoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void postStore(Morphium m, Object r, boolean isNew) {
        Map<Object, Boolean> map = new HashMap<Object, Boolean>();
        map.put(r, isNew);
        sendClearMessage("store", map);
    }

    @Override
    public void postStore(Morphium m, Map<Object, Boolean> isNew) throws MorphiumAccessVetoException {
        sendClearMessage("storeBulk", isNew);
    }


    @Override
    public void postDrop(Morphium m, Class cls) {
        sendClearMessage(cls, "drop");
    }

    @Override
    public void preDrop(Morphium m, Class cls) {
        sendClearMessage(cls, "remove");
    }

    @Override
    public void postRemove(Morphium m, Object r) {
        Map<Object, Boolean> map = new HashMap<Object, Boolean>();
        map.put(r, false);
        sendClearMessage("remove", map);
    }

    @Override
    public void postRemove(Morphium m, List<Object> lst) {
        Map<Object, Boolean> map = new HashMap<Object, Boolean>();
        for (Object r : lst) map.put(r, false);
        sendClearMessage("remove", map);
    }

    @Override
    public void preRemove(Morphium m, Query<Object> q) throws MorphiumAccessVetoException {
    }

    @Override
    public void preRemove(Morphium m, Object r) throws MorphiumAccessVetoException {
    }

    @Override
    public void postLoad(Morphium m, Object o) throws MorphiumAccessVetoException {
    }

    @Override
    public void postLoad(Morphium m, List<Object> o) throws MorphiumAccessVetoException {
    }

    @Override
    public void preUpdate(Morphium m, Class<?> cls, Enum updateType) throws MorphiumAccessVetoException {
    }


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
                                Map<Class<?>, Map<Object, Object>> idCache = morphium.getCache().cloneIdCache();
                                for (Object id : m.getAdditional()) {
                                    if (idCache.get(cls) != null) {
                                        Object toUpdate = idCache.get(cls).get(id);
                                        if (toUpdate == null && (id instanceof String)) {
                                            //Try objectId
                                            toUpdate = idCache.get(cls).get(id);
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
        if (!commitMessage) {
            return null;
        }
        return answer;
    }

    public void disableCommitMessages() {
        commitMessage = false;
    }

    public void enableCommitMessages() {
        commitMessage = true;
    }

    public void setCommitMessage(boolean msg) {
        commitMessage = msg;
    }

    public boolean isCommitMessages() {
        return commitMessage;
    }

}
