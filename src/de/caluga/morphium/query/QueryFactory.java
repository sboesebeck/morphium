package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:08
 * <p/>
 * crate query for a certain type
 */
public interface QueryFactory {
    <T> Query<T> createQuery(Morphium m, Class<? extends T> type);

    @SuppressWarnings("unused")
    Class<? extends Query> getQueryImpl();

    void setQueryImpl(Class<? extends Query> queryImpl);

    @SuppressWarnings("unused")
    void setExecutor(ThreadPoolExecutor ex);

    @SuppressWarnings({"unused", "UnusedParameters"})
    ThreadPoolExecutor getExecutor(Morphium m);
}
