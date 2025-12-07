package de.caluga.morphium.cache;

import de.caluga.morphium.messaging.Msg;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.12
 * Time: 17:25
 * <p>
 * get informed about Cache-Synchronization Events
 */
public interface MessagingCacheSyncListener extends CacheSyncListener {

    /**
     * Class is null for CLEAR ALL
     *
     * @param cls
     * @param m   - message about to be send - add info if necessary!
     * @throws CacheSyncVetoException
     */
    @SuppressWarnings("UnusedParameters")
    void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException;

    @SuppressWarnings("UnusedParameters")
    void postSendClearMsg(Class cls, Msg m);
}
