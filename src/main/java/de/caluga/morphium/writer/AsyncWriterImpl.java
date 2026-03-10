package de.caluga.morphium.writer;

import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 12:36
 * <p>
 */
public class AsyncWriterImpl extends MorphiumWriterImpl {

    @Override
    public <T> Map<String,Object> submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, WriterTask<T> r) {
        if (callback == null) {
            callback = new AsyncOperationCallback<T>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
                }
                @Override
                public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {
                    LoggerFactory.getLogger(AsyncWriterImpl.class).error("Error during async operation", t);
                }
            };
        }

        return super.submitAndBlockIfNecessary(callback, r);
    }
}
