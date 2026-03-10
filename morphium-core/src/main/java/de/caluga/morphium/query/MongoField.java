package de.caluga.morphium.query;

import de.caluga.morphium.MongoType;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.query.geospatial.Geo;
import de.caluga.morphium.query.geospatial.Point;
import de.caluga.morphium.query.geospatial.Polygon;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:30
 * <p>
 * Representation of a field in a query
 */
@SuppressWarnings("UnusedDeclaration")
public interface MongoField<T> {
    Query<T> all(List<Object> val);

    Query<T> all(Object... val);

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

    Query<T> matches(String ptrn, String options);

    Query<T> matches(String ptrn);

    Query<T> type(MongoType t);

    Query<T> in(Collection<?> vals);

    Query<T> nin(Collection<?> vals);

    Query<T> nearSphere(double x, double y);

    Query<T> elemMatch(Map<String, Object> q);

    Query<T> elemMatch(Query<?> q);

    Query<T> near(double x, double y);

    Query<T> nearSphere(double x, double y, double maxDistance);

    Query<T> nearSphere(double x, double y, double minDistance, double maxDistance);

    Query<T> nearSpere(Point point, double minDistance, double maxDistance);

    Query<T> near(double x, double y, double maxDistance);

    /**
     * search for entries with geo coordinates wihtin the given rectancle - x,y upper left, x2,y2 lower right corner
     */
    Query<T> box(double x, double y, double x2, double y2);

    Query<T> polygon(double... p);

    Query<T> center(double x, double y, double r);

    Query<T> centerSphere(double x, double y, double r);


    Query<T> polygon(Polygon p);

    Query<T> getQuery();

    void setQuery(Query<T> q);

    MorphiumObjectMapper getMapper();

    void setMapper(MorphiumObjectMapper mapper);

    Query<T> not();

    String getFieldString();

    void setFieldString(String fld);

    Query<T> bitsAllClear(int... b);

    Query<T> bitsAllSet(int... b);

    Query<T> bitsAnyClear(int... b);

    Query<T> bitsAnySet(int... b);

    Query<T> bitsAllClear(long bitmask);

    Query<T> bitsAllSet(long bitmask);

    Query<T> bitsAnyClear(long bitmask);

    Query<T> bitsAnySet(long bitmask);

    Query<T> near(Point point, double minDistance, double maxDistance);

    Query<T> geoIntersects(Geo shape);

    Query<T> geoWithin(Geo shape);

    Query<T> geoWithinBox(Double x1, Double y1, Double x2, Double y2);
}
