package de.caluga.morphium.query;

import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.MongoType;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.geospatial.Geo;
import de.caluga.morphium.query.geospatial.Point;
import de.caluga.morphium.query.geospatial.Polygon;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:18
 * <p>
 * default implementation of a MongoField
 *
 * @see MongoField
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoFieldImpl<T> implements MongoField<T> {
    private Query<T> query;
    private MorphiumObjectMapper mapper;
    private String fldStr;
    private boolean not = false;

    private FilterExpression fe;


    public MongoFieldImpl() {
    }

    @Override
    public Query<T> not() {
        not = true;
        return query;
    }

    public MongoFieldImpl(Query<T> q, MorphiumObjectMapper map) {
        query = q;
        mapper = map;
    }

    @Override
    public String getFieldString() {
        return fldStr;
    }

    @Override
    public void setFieldString(String fldStr) {
        fe = new FilterExpression();

        // Handle field name translation for dot notation queries
        String translatedFieldName = fldStr;
        if (mapper != null && mapper.getMorphium() != null &&
            mapper.getMorphium().getConfig().isCamelCaseConversionEnabled()) {
            if (fldStr.contains(".")) {
                // Split dot notation and translate each part
                String[] parts = fldStr.split("\\.");
                StringBuilder translatedName = new StringBuilder();

                for (int i = 0; i < parts.length; i++) {
                    if (i == 0) {
                        // Translate the first part (the field name)
                        try {
                            String fieldName = mapper.getMorphium().getARHelper().convertCamelCase(parts[i]);
                            translatedName.append(fieldName);
                        } catch (Exception e) {
                            // If translation fails, use original name
                            translatedName.append(parts[i]);
                        }
                    } else {
                        // Keep subsequent parts as-is (they are sub-field names)
                        translatedName.append(".").append(parts[i]);
                    }
                }
                translatedFieldName = translatedName.toString();
            } else {
                // Single field name - translate normally
                try {
                    translatedFieldName = mapper.getMorphium().getARHelper().convertCamelCase(fldStr);
                } catch (Exception e) {
                    // If translation fails, use original name
                    translatedFieldName = fldStr;
                }
            }
        }

        fe.setField(translatedFieldName);
        this.fldStr = fldStr;
    }

    @Override
    public MorphiumObjectMapper getMapper() {
        return mapper;
    }

    @Override
    public void setMapper(MorphiumObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Query<T> all(List<Object> val) {
        add("$all", val);
        return query;
    }

    @Override
    public Query<T> all(Object... val) {
        return all(Arrays.asList(val));
    }

    @Override
    public Query<T> eq(Object val) {
        // checking for Ids in references...
        val = checkValue(val);

        addSimple(val);
        return query;
    }

    private Object checkValue(Object val) {
        if (val != null) {
            Class<?> cls = val.getClass();
            Field field = mapper.getMorphium().getARHelper().getField(query.getType(), fldStr);
            if (mapper.getMorphium().getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class) || val instanceof MorphiumId) {
                if (field.isAnnotationPresent(Reference.class)) {
                    Object id;
                    if (val instanceof MorphiumId) {
                        id = new ObjectId(val.toString());
                    } else if (val instanceof Enum) {
                        id = ((Enum) val).name();
                    } else {
                        id = mapper.getMorphium().getARHelper().getId(val);
                        if (id instanceof MorphiumId) {
                            id = new ObjectId(id.toString());
                        }
                    }
                    val = id;

                    // list of references, this should be part of
                    //
                    // need to compare DBRefs
                    //                        val = new MorphiumReference(val.getClass().getName(), id);
                    if (Map.class.isAssignableFrom(field.getType()) || Collection.class.isAssignableFrom(field.getType()) || List.class.isAssignableFrom(field.getType())) {
                        fldStr = fldStr + ".refid"; //references in lists - id field in morphiumreference!!!!
                    }
                }

            }
            if (field != null) {
                if (val instanceof MorphiumId && field.getType().equals(String.class)) {
                    val = val.toString();
                } else if (val instanceof String && field.getType().equals(MorphiumId.class)) {
                    try {
                        val = new MorphiumId((String) val);
                    } catch (Exception ignored) {
                    }
                } else if (val instanceof Enum) {
                    //noinspection ConstantConditions
                    val = ((Enum) val).name();
                }
            }
        }
        return val;
    }

    private void addSimple(Object val) {
        if (not) {
            fe.setValue(UtilsMap.of("$not", val));
        } else {
            fe.setValue(val);
        }
        fe.setField(mapper.getMorphium().getARHelper().getMongoFieldName(query.getType(), fldStr));
        query.addChild(fe);
    }


    private void add(String op, Object value) {
        fe.setField(mapper.getMorphium().getARHelper().getMongoFieldName(query.getType(), fldStr));
        FilterExpression child = new FilterExpression();
        child.setField(op);
        if (not) {
            child.setValue(UtilsMap.of("$not", value));
        } else {
            child.setValue(value);
        }
        fe.addChild(child);

        query.addChild(fe);
    }

    private void add(List<FilterExpression> expressionList) {
        fe.setField(mapper.getMorphium().getARHelper().getMongoFieldName(query.getType(), fldStr));
        fe.setChildren(expressionList);
        query.addChild(fe);
    }

    @Override
    public Query<T> ne(Object val) {
        val = checkValue(val);
        add("$ne", val);
        return query;
    }

    @Override
    public Query<T> size(int val) {
        add("$size", val);
        return query;
    }

    @Override
    public Query<T> lt(Object val) {
        val = checkValue(val);
        add("$lt", val);
        return query;
    }

    @Override
    public Query<T> lte(Object val) {
        val = checkValue(val);
        add("$lte", val);
        return query;
    }

    @Override
    public Query<T> gt(Object val) {
        val = checkValue(val);
        add("$gt", val);
        return query;
    }

    @Override
    public Query<T> gte(Object val) {
        val = checkValue(val);
        add("$gte", val);
        return query;
    }

    @Override
    public Query<T> exists() {
        add("$exists", true);
        return query;
    }

    @Override
    public Query<T> notExists() {
        add("$exists", false);
        return query;
    }

    @Override
    public Query<T> mod(int base, int val) {
        List<Object> lst = new ArrayList<>();
        lst.add(base);
        lst.add(val);
        add("$mod", lst);
        return query;
    }

    @SuppressWarnings("CommentedOutCode")
    @Override
    public Query<T> matches(Pattern p) {
        //addSimple(p);
//            fe.setValue(p);
//
//        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
//        query.addChild(fe);
        add("$regex", p.toString());
        if (p.flags() != 0) {
            String options = "";
            if ((p.flags() | Pattern.CASE_INSENSITIVE) != 0) {
                options = options + "i";
            } else if ((p.flags() | Pattern.MULTILINE) != 0) {
                options = options + "m";
            }
            if (!options.isEmpty()) {
                add("$options", options);
            }
        }
        return query;
    }

    @Override
    public Query<T> matches(String ptrn, String options) {
        add("$regex", ptrn);

        if (options != null && !options.isEmpty()) {
            add("$options", options);
        }

        return query;
    }

    @Override
    public Query<T> matches(String ptrn) {
        return matches(ptrn, null);
    }

    @Override
    public Query<T> type(MongoType t) {
        add("$type", t.getId());
        return query;
    }

    @Override
    public Query<T> in(Collection<?> vals) {
        List<Object> lst = new ArrayList<>();
        for (Object v : vals) {
            lst.add(checkValue(v));
        }
        add("$in", lst);
        return query;
    }

    @Override
    public Query<T> nin(Collection<?> vals) {
        List<Object> lst = new ArrayList<>();
        for (Object v : vals) {
            lst.add(checkValue(v));
        }
        add("$nin", lst);
        return query;
    }


    private void createGeoWithinFilterExpression(List<Object> lst, String type) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$geoWithin");

        Map<String, Object> box = new HashMap<>();
        box.put(type, lst);
        withinExpression.setValue(box);

        expressionList.add(withinExpression);

        add(expressionList);
    }

    @Override
    public Query<T> polygon(double... p) {
        if (p.length % 2 == 1) {
            throw new IllegalArgumentException("Need a list of coordinates: x,y, x1,y1, x2,y2....");
        }
        List<Object> lst = new ArrayList<>();
        for (int i = 0; i < p.length; i += 2) {
            List<Object> p1 = new ArrayList<>();
            p1.add(p[i]);
            p1.add(p[i + 1]);
            lst.add(p1);
        }

        createGeoWithinFilterExpression(lst, "$polygon");
        return query;
    }

    @Override
    public Query<T> polygon(Polygon p) {
        //noinspection unchecked
        createGeoWithinFilterExpression((List) p.getCoordinates(), "$polygon");
        return query;
    }

    @Override
    public Query<T> geoWithinBox(Double x1, Double y1, Double x2, Double y2) {
        createGeoWithinFilterExpression(Arrays.asList(Arrays.asList(x1, y1), Arrays.asList(x2, y2)), "$box");
        return query;
    }

    @Override
    public Query<T> getQuery() {
        return query;
    }

    @Override
    public void setQuery(Query<T> q) {
        query = q;
    }

    @Override
    public Query<T> bitsAllClear(int... b) {
        List<Integer> lst = new ArrayList<>();
        for (int r : b) lst.add(r);
        add("$bitsAllClear", lst);
        return query;
    }

    @Override
    public Query<T> bitsAllSet(int... b) {
        List<Integer> lst = new ArrayList<>();
        for (int r : b) lst.add(r);
        add("$bitsAllSet", lst);
        return query;
    }

    @Override
    public Query<T> bitsAnyClear(int... b) {
        List<Integer> lst = new ArrayList<>();
        for (int r : b) lst.add(r);
        add("$bitsAnyClear", lst);
        return query;
    }

    @Override
    public Query<T> bitsAnySet(int... b) {
        List<Integer> lst = new ArrayList<>();
        for (int r : b) lst.add(r);
        add("$bitsAnySet", lst);
        return query;
    }

    @Override
    public Query<T> bitsAllClear(long bitmask) {

        add("$bitsAllClear", bitmask);
        return query;
    }

    @Override
    public Query<T> bitsAllSet(long bitmask) {

        add("$bitsAllSet", bitmask);
        return query;
    }

    @Override
    public Query<T> bitsAnyClear(long bitmask) {

        add("$bitsAnyClear", bitmask);
        return query;
    }

    @Override
    public Query<T> bitsAnySet(long bitmask) {
        add("$bitsAnySet", bitmask);
        return query;
    }

    @Override
    public Query<T> elemMatch(Map<String, Object> q) {
        add("$elemMatch", q);
        return query;
    }

    @Override
    public Query<T> elemMatch(Query<?> q) {
        return elemMatch(q.toQueryObject());
    }
//Geospacial queries

    @Override
    public Query<T> near(double x, double y) {
        List<Object> lst = new ArrayList<>();
        lst.add(x);
        lst.add(y);
        add("$near", lst);
        return query;
    }


    @Override
    public Query<T> nearSphere(double x, double y) {
        List<Object> lst = new ArrayList<>();
        lst.add(x);
        lst.add(y);
        add("$nearSphere", lst);
        return query;
    }

    @Override
    /**
     * search for entries with geo coordinates wihtin the given rectancle - x,y upper left, x2,y2 lower right corner
     */
    public Query<T> box(double x, double y, double x2, double y2) {
        return geoWithinBox(x, y, x2, y2);
    }

    @Override
    public Query<T> center(double x, double y, double r) {
        List<Object> lst = new ArrayList<>();
        List<Object> p1 = new ArrayList<>();
        p1.add(x);
        p1.add(y);

        lst.add(p1);
        lst.add(r);

        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$geoWithin");

        HashMap<String, Object> cnt = new HashMap<>();
        cnt.put("$center", lst);
        withinExpression.setValue(cnt);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> centerSphere(double x, double y, double r) {
        List<Object> lst = new ArrayList<>();
        List<Object> p1 = new ArrayList<>();
        p1.add(x);
        p1.add(y);

        lst.add(p1);
        lst.add(r);

        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$geoWithin");

        HashMap<String, Object> cnt = new HashMap<>();
        cnt.put("$centerSphere", lst);
        withinExpression.setValue(cnt);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> nearSphere(double x, double y, double maxDistance) {
        return nearSphere(x, y, 0, maxDistance);

    }

    @Override
    public Query<T> nearSphere(double x, double y, double minDistance, double maxDistance) {
        List<Object> location = new ArrayList<>();
        location.add(x);
        location.add(y);

        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$nearSphere");
        nearExpression.setValue(location);
        expressionList.add(nearExpression);

        FilterExpression maxDistanceExpression = new FilterExpression();
        maxDistanceExpression.setField("$maxDistance");
        maxDistanceExpression.setValue(maxDistance);
        expressionList.add(maxDistanceExpression);

        FilterExpression minDistanceExpression = new FilterExpression();
        maxDistanceExpression.setField("$minDistance");
        maxDistanceExpression.setValue(minDistance);
        expressionList.add(maxDistanceExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> nearSpere(Point point, double minDistance, double maxDistance) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$nearSphere");
        Map<String, Object> val = UtilsMap.of("$geometry", mapper.serialize(point));
        val.put("$maxDistance", maxDistance);
        val.put("$minDistance", minDistance);
        nearExpression.setValue(val);
        expressionList.add(nearExpression);

        add(expressionList);

        return query;
    }


    @Override
    public Query<T> near(double x, double y, double maxDistance) {
        List<Object> location = new ArrayList<>();
        location.add(x);
        location.add(y);

        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$near");
        nearExpression.setValue(location);
        expressionList.add(nearExpression);

        FilterExpression maxDistanceExpression = new FilterExpression();
        maxDistanceExpression.setField("$maxDistance");
        maxDistanceExpression.setValue(maxDistance);
        expressionList.add(maxDistanceExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> near(Point point, double minDistance, double maxDistance) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$near");
        Map<String, Object> val = UtilsMap.of("$geometry", mapper.serialize(point));
        val.put("$maxDistance", maxDistance);
        val.put("$minDistance", minDistance);
        nearExpression.setValue(val);
        expressionList.add(nearExpression);

        add(expressionList);

        return query;
    }

    @Override
    public Query<T> geoIntersects(Geo shape) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression expr = new FilterExpression();
        expr.setField("$geoIntersects");
        Map<String, Map<String, Object>> val = UtilsMap.of("$geometry", mapper.serialize(shape));

        expr.setValue(val);
        expressionList.add(expr);

        add(expressionList);

        return query;
    }

    @Override
    public Query<T> geoWithin(Geo shape) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression expr = new FilterExpression();
        expr.setField("$geoWithin");
        Map<String, Map<String, Object>> val = UtilsMap.of("$geometry", mapper.serialize(shape));

        expr.setValue(val);
        expressionList.add(expr);

        add(expressionList);

        return query;
    }


}
