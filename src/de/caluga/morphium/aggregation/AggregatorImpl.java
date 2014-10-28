package de.caluga.morphium.aggregation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:24
 * <p/>
 */
public class AggregatorImpl<T, R> implements Aggregator<T, R> {
    private Class<? extends T> type;
    private List<DBObject> params = new ArrayList<DBObject>();
    private Morphium morphium;
    private Class<? extends R> rType;


    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    @Override
    public void setSearchType(Class<? extends T> type) {
        this.type = type;
    }

    @Override
    public Class<? extends T> getSearchType() {
        return type;
    }

    @Override
    public void setResultType(Class<? extends R> type) {
        rType = type;
    }

    @Override
    public Class<? extends R> getResultType() {
        return rType;
    }

    @Override
    public Aggregator<T, R> project(Map<String, Object> m) {
        DBObject o = new BasicDBObject("$project", new BasicDBObject(m));
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> project(String... m) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (String sm : m) {
            map.put(sm, 1);
        }
        return project(map);
    }

    @Override
    public Aggregator<T, R> project(BasicDBObject m) {
        DBObject o = new BasicDBObject("$project", new BasicDBObject(m));
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        DBObject o = new BasicDBObject("$match", q.toQueryObject());
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> limit(int num) {
        DBObject o = new BasicDBObject("$limit", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> skip(int num) {
        DBObject o = new BasicDBObject("$skip", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        DBObject o = new BasicDBObject("$unwind", listField);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> sort(String... prefixed) {
        Map<String, Integer> m = new LinkedHashMap<String, java.lang.Integer>();
        for (String i : prefixed) {
            String fld = i;
            int val = 1;
            if (i.startsWith("-")) {
                fld = i.substring(1);
                val = -1;
            } else if (i.startsWith("+")) {
                fld = i.substring(1);
                val = 1;
            }
            if (i.startsWith("$")) {
                fld = fld.substring(1);
                if (!fld.contains(".")) {
                    fld = morphium.getARHelper().getFieldName(type, fld);
                }
            }
            m.put(fld, val);
        }
        sort(m);
        return this;
    }

    @Override
    public Aggregator<T, R> sort(Map<String, Integer> sort) {
        DBObject o = new BasicDBObject("$sort", new BasicDBObject(sort));
        params.add(o);
        return this;
    }

    @Override
    public Group<T, R> group(BasicDBObject id) {
        return new Group<T, R>(this, id);
    }

    @Override
    public Group<T, R> group(Map<String, String> idSubObject) {
        return new Group<T, R>(this, idSubObject);
    }

    @Override
    public Group<T, R> group(String id) {
        return new Group<T, R>(this, id);
    }

    @Override
    public void addOperator(DBObject o) {
        params.add(o);
    }

    @Override
    public List<R> aggregate() {
        return morphium.aggregate(this);
    }

    @Override
    public void aggregate(final AsyncOperationCallback<R> callback) {
        if (callback == null) {
            morphium.aggregate(this);
        } else {

            morphium.queueTask(new Runnable() {
                @Override
                public void run() {
                    long start = System.currentTimeMillis();
                    List<R> ret = morphium.aggregate(AggregatorImpl.this);
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, ret, null, AggregatorImpl.this);
                }
            });
        }
    }

    @Override
    public List<DBObject> toAggregationList() {
        return params;
    }
}
