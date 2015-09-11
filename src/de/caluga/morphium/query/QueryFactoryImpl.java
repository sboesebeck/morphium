package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;

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
    private ThreadPoolExecutor executor = null;

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
    public ThreadPoolExecutor getExecutor(Morphium m) {
        if (executor == null) {
            executor = new ThreadPoolExecutor(m.getConfig().getMaxConnections() / 2, (int) (m.getConfig().getMaxConnections() * m.getConfig().getBlockingThreadsMultiplier() * 0.9),
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
            m.addShutdownListener(new ShutdownListener() {
                @Override
                public void onShutdown(Morphium m) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        //swallow
                    }
                }
            });
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
            q.setExecutor(getExecutor(m));
            return q;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
