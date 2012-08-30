package de.caluga.morphium.query;

import de.caluga.morphium.MongoType;
import de.caluga.morphium.ObjectMapper;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:30
 * <p/>
 * TODO: Add documentation here
 */
public interface MongoField<T> {
    public Query<T> eq(Object val);

    public Query<T> ne(Object val);

    public Query<T> lt(Object val);

    public Query<T> lte(Object val);

    public Query<T> gt(Object val);

    public Query<T> gte(Object val);

    public Query<T> exists();

    public Query<T> notExists();

    public Query<T> mod(int base, int val);

    public Query<T> matches(Pattern p);

    public Query<T> matches(String ptrn);

    public Query<T> type(MongoType t);

    public Query<T> in(Collection<?> vals);

    public Query<T> nin(Collection<?> vals);

    public Query<T> near(double x, double y);

    public Query<T> getQuery();

    public void setQuery(Query<T> q);

    public ObjectMapper getMapper();

    public void setMapper(ObjectMapper mapper);

    public String getFieldString();

    public void setFieldString(String fld);
}
