package de.caluga.morphium.cache;

import de.caluga.morphium.ConfigElement;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgType;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.Hashtable;

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
public class CacheSynchronizer implements MorphiumStorageListener<Object>, MessageListener {
    private static final Logger log = Logger.getLogger(CacheSynchronizer.class);

    private Messaging messaging;
    private Morphium morphium;

    public static final String CACHE_SYNC_TYPE = "cacheSyncType";
    public static final String CACHE_SYNC_RECORD = "cacheSyncRecord";

    public CacheSynchronizer(Messaging msg, Morphium morphium) {
        messaging = msg;
        this.morphium = morphium;

        morphium.addListener(this);

        messaging.addListenerForMessageNamed(CACHE_SYNC_TYPE, this);
        messaging.addListenerForMessageNamed(CACHE_SYNC_RECORD, this);

    }


//    public void sendClearMessage(List<?> recLst, Map<Object,Boolean> isNew, String reason) {
//        //sort by type :-(
//        Hashtable<Class<?>, List<String>> sorted = new Hashtable<Class<?>, List<String>>();
//        for (Object r : recLst) {
//            if (isNew.get(r)) continue;
//            if (sorted.get(r.getClass()) == null) {
//                sorted.put(r.getClass(), new ArrayList<String>());
//            }
//            sorted.get(r.getClass()).add(morphium.getId(r).toString());
//        }
//
//        //running through
//        for (Class type : sorted.keySet()) {
//            Cache c = morphium.getAnnotationFromHierarchy(type, Cache.class);
//            if (type.equals(ConfigElement.class) || (c != null && c.readCache())) {
//                if (c.syncCache().equals(Cache.SyncCacheStrategy.UPDATE_ENTRY)) {
//                    Msg m = new Msg(CACHE_SYNC_RECORD, MsgType.MULTI, reason, type.getName(), 30000);
//                    m.setAdditional(sorted.get(type));
//                    messaging.queueMessage(m);
//                } else if (c.syncCache().equals(Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)) {
//                    sendClearMessage(type, reason);
//                }
//            }
//        }
//    }


    public void sendClearMessage(Object record, String reason, boolean isNew) {
        long start = System.currentTimeMillis();
        if (record.equals(Msg.class)) return;
        ObjectId id = morphium.getId(record);

        Msg m = new Msg(CACHE_SYNC_RECORD, MsgType.MULTI, reason, record.getClass().getName(), 30000);
        if (id != null)
            m.addAdditional(id.toString());
        if (record.equals(ConfigElement.class)) {
            messaging.queueMessage(m);
            return;
        }
        Cache c = morphium.getAnnotationFromHierarchy(record.getClass(), Cache.class); //(Cache) type.getAnnotation(Cache.class);
        if (c == null) return; //not clearing cache for non-cached objects
        if (c.readCache() && c.clearOnWrite()) {
            if (c.syncCache().equals(Cache.SyncCacheStrategy.UPDATE_ENTRY) || c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
                if (!isNew)
                    messaging.queueMessage(m);
            } else if (c.syncCache().equals(Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE)) {
                m.setName(CACHE_SYNC_TYPE);
                messaging.queueMessage(m);
            }
        }
//        long dur = System.currentTimeMillis() - start;
//        log.info("Queueing cache sync message took "+dur+" ms");
    }

    /**
     * sends message if necessary
     *
     * @param type
     * @param reason
     */
    public void sendClearMessage(Class type, String reason) {
        if (type.equals(Msg.class)) return;
        if (type.equals(ConfigElement.class)) {
            Msg m = new Msg(CACHE_SYNC_TYPE, MsgType.MULTI, reason, type.getName(), 30000);
            messaging.queueMessage(m);
            return;
        }
        Cache c = morphium.getAnnotationFromHierarchy(type, Cache.class); //(Cache) type.getAnnotation(Cache.class);
        if (c == null) return; //not clearing cache for non-cached objects
        if (c.readCache() && c.clearOnWrite()) {
            if (!c.syncCache().equals(Cache.SyncCacheStrategy.NONE)) {
                sendClearMessage(type.getName(), reason);
            }
        }
    }

    public void detach() {
        morphium.removeListener(this);
        messaging.removeListenerForMessageNamed(CACHE_SYNC_TYPE, this);
        messaging.removeListenerForMessageNamed(CACHE_SYNC_RECORD, this);
    }

    /**
     * always sends message
     *
     * @param type
     * @param reason
     */

    public void sendClearMessage(String type, String reason) {
        Msg m = new Msg(CACHE_SYNC_TYPE, MsgType.MULTI, reason, type, 30000);
        messaging.queueMessage(m);
    }


    @Override
    public void preStore(Object r, boolean isNew) {
        //ignore
    }

    @Override
    public void postStore(Object r, boolean isNew) {
        sendClearMessage(r, "store", isNew);
    }

    @Override
    public void postRemove(Object r) {
        sendClearMessage(r.getClass(), "remove");
    }

    @Override
    public void preDelete(Object r) {
        //ignore
    }

    @Override
    public void postDrop(Class cls) {
        sendClearMessage(cls, "drop");
    }

    @Override
    public void preDrop(Class cls) {
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
    public void preRemove(Query q) {
    }

    @Override
    public void postRemove(Query q) {
        sendClearMessage(q.getType(), "remove");
    }

    @Override
    public void postLoad(Object o) {
        //ignore
    }

    @Override
    public void preUpdate(Class cls, Enum updateType) {
        //ignore
    }

    @Override
    public void postUpdate(Class cls, Enum updateType) {
        sendClearMessage(cls, "Update: " + updateType.name());
    }

    @Override
    public Msg onMessage(Msg m) {
        Msg answer = new Msg("clearCacheAnswer", "processed", messaging.getSenderId());
        try {
            if (log.isDebugEnabled()) {
                String action = m.getMsg();
                String sender = m.getSender();
                log.debug("Got message " + m.getName() + " from " + sender + " - Action: " + action + " Class: " + m.getValue());
            }
            if (m.getName().equals(CACHE_SYNC_TYPE)) {
                if (m.getValue().equals("ALL")) {
                    log.info("Cache completely cleared");
                    morphium.resetCache();
                    answer.setMsg("cache completely cleared");
                    return answer;
                }
                Class cls = Class.forName(m.getValue());
                if (cls.equals(ConfigElement.class)) {
                    morphium.getConfigManager().reinitSettings();
                    answer.setMsg("config reread");
                    return answer;
                }
                if (morphium.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                    Cache c = morphium.getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
                    if (c != null) {
                        if (c.readCache()) {
                            //Really clearing cache, even if clear on write is set to false! => manual clearing?
                            morphium.clearCachefor(cls);
                            answer.setMsg("cache cleared for type: " + m.getValue());
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
                if (morphium.isAnnotationPresentInHierarchy(cls, Entity.class)) {
                    Cache c = morphium.getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
                    if (c != null) {
                        if (c.readCache()) {
                            Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> idCache = morphium.cloneIdCache();
                            for (String a : m.getAdditional()) {
                                ObjectId id = new ObjectId(a);
                                if (idCache.get(cls) != null) {
                                    if (idCache.get(cls).get(id) != null) {
                                        //Object is updated in place!
                                        if (c.syncCache().equals(Cache.SyncCacheStrategy.REMOVE_ENTRY_FROM_TYPE_CACHE)) {
                                            morphium.removeEntryFromCache(cls, id);
                                        } else {
                                            morphium.reread(idCache.get(cls).get(id));

                                        }
                                    }
                                }
                            }
                            morphium.setIdCache(idCache);
                            answer.setMsg("cache cleared for type: " + m.getValue());

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

    @Override
    public void setMessaging(Messaging msg) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
