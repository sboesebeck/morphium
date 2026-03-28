package de.caluga.morphium.writer;

import java.util.Map;

import de.caluga.morphium.async.AsyncOperationCallback;

/**
 * User: Stephan Bösebeck
 * Date: 28.06.13
 * Time: 16:51
 * <p>
 * a writer task
 * </p>
 *
 * @param <T> the type of the entity
 */
@SuppressWarnings("WeakerAccess")
public interface WriterTask<T> extends Runnable {
    default Map<String,Object> getReturnObject(){
        return null;
    }
    void setCallback(AsyncOperationCallback<T> cb);
}
