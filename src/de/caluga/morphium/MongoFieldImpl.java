package de.caluga.morphium;

import com.mongodb.BasicDBList;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:18
 * <p/>
 * TODO: Add documentation here
 */
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
    public Query<T> eq(Object val) {

        fe.setValue(val);
        fe.setField(fldStr);
        query.addChild(fe);
        return query;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void add(String op, Object value) {
        fe.setField(fldStr);
        FilterExpression child = new FilterExpression();
        child.setField(op);
        child.setValue(value);
        fe.addChild(child);
        query.addChild(fe);
    }

    @Override
    public Query<T> ne(Object val) {
        add("$ne", val);
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
        BasicDBList lst = new BasicDBList();
        lst.add(base);
        lst.add(val);
        add("$mod", lst);
        return query;
    }

    @Override
    public Query<T> matches(Pattern p) {
        //$regex : 'acme.*corp', $options: 'i'
        add("$regex", p.toString());
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
        BasicDBList lst = new BasicDBList();
        lst.addAll(vals);
        add("$in", lst);
        return query;
    }

    @Override
    public Query<T> nin(Collection<?> vals) {
        BasicDBList lst = new BasicDBList();
        lst.addAll(vals);
        add("$nin", lst);
        return query;
    }

    @Override
    public Query<T> near(double x, double y) {
        BasicDBList lst = new BasicDBList();
        lst.add(x);
        lst.add(y);
        add("$near", lst);
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
