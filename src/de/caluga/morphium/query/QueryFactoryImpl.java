package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:03
 * <p/>
 * default implementation of the query factory. Override for own use and set it to MorphiumConfig
 */
public class QueryFactoryImpl implements QueryFactory {
    private Class<? extends Query> queryImpl;

    public QueryFactoryImpl(Class<? extends Query> qi) {
        queryImpl = qi;
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
    public <T> Query<T> createQuery(Morphium m, Class<T> type) {
        try {
            Query<T> q = (Query<T>) queryImpl.newInstance();
            q.setMorphium(m);
            q.setType(type);
            return q;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
