package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:03
 * <p/>
 * default implementation of the query factory. Override for own use and set it to MorphiumConfig
 */
public class QueryFactoryImpl implements QueryFactory {
    private Class<? extends Query> queryImpl;
    private ThreadPoolExecutor executor = null;

    @SuppressWarnings("unused")
    public QueryFactoryImpl() {
    }

    public QueryFactoryImpl(Class<? extends Query> qi) {
        queryImpl = qi;
    }

    @Override
    public void setExecutor(ThreadPoolExecutor ex) {
        executor = ex;
    }

    @Override
    public ThreadPoolExecutor getExecutor(Morphium m) {
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
            q.setExecutor(m.getAsyncOperationsThreadPool());
            return q;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
