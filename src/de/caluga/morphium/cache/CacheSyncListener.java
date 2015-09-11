package de.caluga.morphium.cache;

import de.caluga.morphium.messaging.Msg;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.12
 * Time: 17:25
 * <p/>
 * get informed about Cache-Synchronization Events
 */
public interface CacheSyncListener {

    /**
     * before clearing cache - if cls == null whole cache
     * Message m contains information about reason and stuff...
     */
    void preClear(Class cls, Msg m) throws CacheSyncVetoException;

    void postClear(Class cls, Msg m);

    /**
     * Class is null for CLEAR ALL
     *
     * @param cls
     * @param m   - message about to be send - add info if necessary!
     * @throws CacheSyncVetoException
     */
    void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException;

    void postSendClearMsg(Class cls, Msg m);
}
