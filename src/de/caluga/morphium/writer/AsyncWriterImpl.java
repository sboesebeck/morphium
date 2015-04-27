package de.caluga.morphium.writer;

import de.caluga.morphium.Logger;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 12:36
 * <p/>
 * TODO: Add documentation here
 */
public class AsyncWriterImpl extends MorphiumWriterImpl {
    @Override
    public <T> void submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, WriterTask<T> r) {
        if (callback == null) {
            callback = new AsyncOperationCallback<T>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {
                    new Logger(AsyncWriterImpl.class).error("Error during async operation", t);
                }
            };
        }
        super.submitAndBlockIfNecessary(callback, r);
    }
}
