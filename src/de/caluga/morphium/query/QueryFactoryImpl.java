package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:03
 * <p/>
 * default implementation of the query factory. Override for own use and set it to MorphiumConfig
 */
public class QueryFactoryImpl implements QueryFactory {
    private Class<? extends Query> queryImpl;
    ThreadPoolExecutor executor = null;

    public QueryFactoryImpl() {
    }

    @Override
    public void setExecutor(ThreadPoolExecutor ex) {
        executor = ex;
    }

    public QueryFactoryImpl(Class<? extends Query> qi) {
        queryImpl = qi;
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
        if (executor == null) {
            executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }
        return executor;
    }

    @Override
    public Class<? extends Query> getQueryImpl() {
        return queryImpl;
    }

    @Override
    public void setQueryImpl(Class<? extends Query> queryImpl) {
        this.queryImpl = queryImpl;
    }

    @Override
    public <T> Query<T> createQuery(Morphium m, Class<? extends T> type) {
        try {
            Query<T> q = queryImpl.newInstance();
            q.setMorphium(m);
            q.setType(type);
            q.setExecutor(getExecutor());

            return q;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
