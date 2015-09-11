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
    Query<T> all(List<Object> val);

    Query<T> eq(Object val);

    Query<T> ne(Object val);

    Query<T> size(int val);

    Query<T> lt(Object val);

    Query<T> lte(Object val);

    Query<T> gt(Object val);

    Query<T> gte(Object val);

    Query<T> exists();

    Query<T> notExists();

    Query<T> mod(int base, int val);

    Query<T> matches(Pattern p);

    Query<T> matches(String ptrn);

    Query<T> type(MongoType t);

    Query<T> in(Collection<?> vals);

    Query<T> nin(Collection<?> vals);

    /**
     * return a sorted list of elements around point x,y
     * spherical distance calculation
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    Query<T> nearSphere(double x, double y);

    /**
     * return a sorted list of elements around point x,y
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    Query<T> near(double x, double y);

    /**
     * return a sorted list of elements around point x,y
     * spherical distance calculation
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    Query<T> nearSphere(double x, double y, double maxDistance);

    /**
     * return a sorted list of elements around point x,y
     *
     * @param x pos x
     * @param y pos y
     * @return the query
     */
    Query<T> near(double x, double y, double maxDistance);

    /**
     * search for entries with geo coordinates wihtin the given rectancle - x,y upper left, x2,y2 lower right corner
     */
    Query<T> box(double x, double y, double x2, double y2);

    Query<T> polygon(double... p);

    Query<T> center(double x, double y, double r);

    /**
     * same as center() but uses spherical geometry for distance calc.
     *
     * @param x - pos x
     * @param y - y pos
     * @param r - radius
     * @return the query
     */
    Query<T> centerSphere(double x, double y, double r);


    Query<T> getQuery();

    void setQuery(Query<T> q);

    ObjectMapper getMapper();

    void setMapper(ObjectMapper mapper);

    String getFieldString();

    void setFieldString(String fld);
}
