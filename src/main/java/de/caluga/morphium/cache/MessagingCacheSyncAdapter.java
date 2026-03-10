package de.caluga.morphium.cache;

import de.caluga.morphium.messaging.Msg;

/**
 * User: Stephan Bösebeck
 * Date: 14.07.12
 * Time: 18:19
 * <p>
 */
@SuppressWarnings("UnusedDeclaration")
public class MessagingCacheSyncAdapter implements MessagingCacheSyncListener {
    @Override
    public void preClear(Class cls) {
    }

    @Override
    public void postClear(Class cls) {
    }

    @Override
    public void preSendClearMsg(Class cls, Msg m) {
    }

    @Override
    public void postSendClearMsg(Class cls, Msg m) {
    }
}
