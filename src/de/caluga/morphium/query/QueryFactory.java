package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:08
 * <p/>
 * TODO: Add documentation here
 */
public interface QueryFactory {
    public <T> Query<T> createQuery(Morphium m, Class<T> type);

    public Class<? extends Query> getQueryImpl();

    public void setQueryImpl(Class<? extends Query> queryImpl);
}
