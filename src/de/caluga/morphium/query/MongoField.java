package de.caluga.morphium.query;

import de.caluga.morphium.MongoType;
import de.caluga.morphium.ObjectMapper;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:30
 * <p/>
 * Representation of a field in a query
 */
@SuppressWarnings("UnusedDeclaration")
public interface MongoField<T> {
    public Query<T> all(List<Object> val);

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

    /**
     * return a sorted list of elements around point x,y
     * spherical distance calculation
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    public Query<T> nearSphere(double x, double y);

    /**
     * return a sorted list of elements around point x,y
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    public Query<T> near(double x, double y);

    /**
     * return a sorted list of elements around point x,y
     * spherical distance calculation
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    public Query<T> nearSphere(double x, double y, double maxDistance);

    /**
     * return a sorted list of elements around point x,y
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    public Query<T> near(double x, double y, double maxDistance);

    /**
     * search for entries with geo coordinates wihtin the given rectancle - x,y upper left, x2,y2 lower right corner
     */
    public Query<T> box(double x, double y, double x2, double y2);

    public Query<T> polygon(double... p);

    public Query<T> center(double x, double y, double r);

    /**
     * same as center() but uses spherical geometry for distance calc.
     *
     * @param x - pos x
     * @param y - y pos
     * @param r - radius
     * @return the query
     */
    public Query<T> centerSphere(double x, double y, double r);


    public Query<T> getQuery();

    public void setQuery(Query<T> q);

    public ObjectMapper getMapper();

    public void setMapper(ObjectMapper mapper);

    public String getFieldString();

    public void setFieldString(String fld);
}
