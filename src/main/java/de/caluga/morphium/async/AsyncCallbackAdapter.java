package de.caluga.morphium.async;

/**
 * Created by stephan on 29.07.16.
 */

import de.caluga.morphium.query.Query;

import java.util.List;

public class AsyncCallbackAdapter<T> implements AsyncOperationCallback<T> {
    @Override
    public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {

    }

    @Override
    public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {

    }
}
