package de.caluga.morphium.query;

import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.MongoType;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.bson.MorphiumId;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:18
 * <p/>
 * default implementation of a MongoField
 *
 * @see MongoField
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoFieldImpl<T> implements MongoField<T> {
    private Query<T> query;
    private ObjectMapper mapper;
    private String fldStr;

    private FilterExpression fe;


    public MongoFieldImpl() {
    }

    public MongoFieldImpl(Query<T> q, ObjectMapper map) {
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
        fe.setField(fldStr);
        this.fldStr = fldStr;
    }

    @Override
    public ObjectMapper getMapper() {
        return mapper;
    }

    @Override
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Query<T> all(List<Object> val) {
        add("$all", val);
        return query;
    }

    @Override
    public Query<T> eq(Object val) {
        // checking for Ids in references...
        if (val != null) {
            Class<?> cls = val.getClass();
            Field field = mapper.getMorphium().getARHelper().getField(query.getType(), fldStr);
            if (mapper.getMorphium().getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class) || val instanceof MorphiumId) {
                if (field.isAnnotationPresent(Reference.class)) {
                    Object id;
                    if (val instanceof MorphiumId) {
                        id = val;
                    } else {
                        id = mapper.getMorphium().getARHelper().getId(val);
                    }
                    val = id;
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        // list of references, this should be part of
                        //
                        // need to compare DBRefs
                        //                        val = new MorphiumReference(val.getClass().getName(), id);
                        fldStr = fldStr + ".id"; //references in lists - id field in morphiumreference!!!!
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
                }
            }
        }

        fe.setValue(val);
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        query.addChild(fe);
        return query;  // To change body of implemented methods use File | Settings | File Templates.
    }

    private void add(String op, Object value) {
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        FilterExpression child = new FilterExpression();
        child.setField(op);
        child.setValue(value);
        fe.addChild(child);

        query.addChild(fe);
    }

    private void add(List<FilterExpression> expressionList) {
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        fe.setChildren(expressionList);
        query.addChild(fe);
    }

    @Override
    public Query<T> ne(Object val) {
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
        add("$lt", val);
        return query;
    }

    @Override
    public Query<T> lte(Object val) {
        add("$lte", val);
        return query;
    }

    @Override
    public Query<T> gt(Object val) {
        add("$gt", val);
        return query;
    }

    @Override
    public Query<T> gte(Object val) {
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

    @Override
    public Query<T> matches(Pattern p) {
        fe.setValue(p);
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        query.addChild(fe);
        return query;
    }

    @Override
    public Query<T> matches(String ptrn) {
        return matches(Pattern.compile(ptrn));
    }

    @Override
    public Query<T> type(MongoType t) {
        add("$type", t.getNumber());
        return query;
    }

    @Override
    public Query<T> in(Collection<?> vals) {
        List<Object> lst = new ArrayList<>();
        lst.addAll(vals);
        add("$in", lst);
        return query;
    }

    @Override
    public Query<T> nin(Collection<?> vals) {
        List<Object> lst = new ArrayList<>();
        lst.addAll(vals);
        add("$nin", lst);
        return query;
    }

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
        List<Object> lst = new ArrayList<>();
        List<Object> p1 = new ArrayList<>();
        p1.add(x);
        p1.add(y);
        List<Object> p2 = new ArrayList<>();
        p2.add(x2);
        p2.add(y2);

        lst.add(p1);
        lst.add(p2);

        createFilterExpressionList(lst, "$box");
        return query;
    }

    private void createFilterExpressionList(List<Object> lst, String type) {
        List<FilterExpression> expressionList = new ArrayList<>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$within");

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

        createFilterExpressionList(lst, "$polygon");
        return query;
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
        withinExpression.setField("$within");

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
        withinExpression.setField("$within");

        HashMap<String, Object> cnt = new HashMap<>();
        cnt.put("$centerSphere", lst);
        withinExpression.setValue(cnt);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> nearSphere(double x, double y, double maxDistance) {
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
    public Query<T> getQuery() {
        return query;
    }

    @Override
    public void setQuery(Query<T> q) {
        query = q;
    }

}
