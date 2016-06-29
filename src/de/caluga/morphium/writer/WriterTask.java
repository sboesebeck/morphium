package de.caluga.morphium.writer;

import de.caluga.morphium.async.AsyncOperationCallback;

/**
 * User: Stephan Bösebeck
 * Date: 28.06.13
 * Time: 16:51
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("WeakerAccess")
public interface WriterTask<T> extends Runnable {
    void setCallback(AsyncOperationCallback<T> cb);
}
