package de.caluga.morphium.annotations.caching;

import de.caluga.morphium.ConfigElement;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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
public class CacheSynchronizer implements MorphiumStorageListener, MessageListener {
    private static final Logger log = Logger.getLogger(CacheSynchronizer.class);

    private Messaging messaging;
    private Morphium morphium;

    public static final String MSG_NAME = "cacheSync";

    public CacheSynchronizer(Messaging msg, Morphium morphium) {
        messaging = msg;
        this.morphium = morphium;

        morphium.addListener(this);

        messaging.addListenerForMessageNamed(MSG_NAME, this);
    }

    public void sendClearMessage(Class type, String reason) {
        if (type.equals(Msg.class)) return;
        if (type.equals(ConfigElement.class)) {
            Msg m = new Msg(MSG_NAME, MsgType.MULTI, reason, null, type.getName(), 30000);
            messaging.queueMessage(m);
            return;
        }
        Cache c = (Cache) type.getAnnotation(Cache.class);
        if (c == null) return; //not clearing cache for non-cached objects
        if (c.readCache() && c.clearOnWrite()) {
            Msg m = new Msg(MSG_NAME, MsgType.MULTI, reason, null, type.getName(), 30000);
            messaging.queueMessage(m);
        }
    }

    @Override
    public void preStore(Object r) {
        //ignore
    }

    @Override
    public void postStore(Object r) {
        sendClearMessage(r.getClass(), "store");
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

    @Override
    public void postListStore(List lst) {
        List<Class> toClear = new ArrayList<Class>();
        for (Object o : lst) {
            if (!toClear.contains(o.getClass())) {
                toClear.add(o.getClass());
            }
        }
        for (Class c : toClear) {
            sendClearMessage(c, "store");
        }
    }

    @Override
    public void preListStore(List lst) {
    }

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
    public void onMessage(Msg m) {
        try {
            if (log.isDebugEnabled()) {
                String action = m.getMsg();
                String sender = m.getSender();
                log.debug("Got message from " + sender + " - Action: " + action + " Class: " + m.getValue());
            }
            Class cls = Class.forName(m.getValue());
            if (cls.equals(ConfigElement.class)) {
                morphium.getConfigManager().reinitSettings();
                return;
            }
            if (cls.isAnnotationPresent(Entity.class)) {
                if (cls.isAnnotationPresent(Cache.class)) {
                    Cache c = (Cache) cls.getAnnotation(Cache.class);
                    if (c.readCache() && c.clearOnWrite()) {
                        morphium.clearCachefor(cls);
                    } else {
                        log.warn("trying to clear cache for uncached enitity or one where clearOnWrite is false");
                    }
                }
            } else {
                log.warn("Trying to clear cache for none-Entity?????");

            }
        } catch (ClassNotFoundException e) {
            log.warn("Could not process message for class " + m.getValue() + " - not found!");
        }
    }
}
