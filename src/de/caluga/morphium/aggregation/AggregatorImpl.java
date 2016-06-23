package de.caluga.morphium.aggregation;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
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
    private final List<Map<String, Object>> params = new ArrayList<>();
    private Class<? extends T> type;
    private Morphium morphium;
    private Class<? extends R> rType;
    private String collectionName;
    private boolean useDisk = false;
    private boolean explain = false;

    @Override
    public boolean isUseDisk() {
        return useDisk;
    }

    @Override
    public void setUseDisk(boolean useDisk) {
        this.useDisk = useDisk;
    }

    @Override
    public boolean isExplain() {
        return explain;
    }

    @Override
    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
    }

    @Override
    public Class<? extends T> getSearchType() {
        return type;
    }

    @Override
    public void setSearchType(Class<? extends T> type) {
        this.type = type;
    }

    @Override
    public Class<? extends R> getResultType() {
        return rType;
    }

    @Override
    public void setResultType(Class<? extends R> type) {
        rType = type;
    }

    @Override
    public Aggregator<T, R> project(String... m) {
        Map<String, Object> map = new HashMap<>();
        for (String sm : m) {
            map.put(sm, 1);
        }
        return project(map);
    }

    @Override
    public Aggregator<T, R> project(Map<String, Object> m) {
        Map<String, Object> o = Utils.getMap("$project", m);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        Map<String, Object> o = Utils.getMap("$match", q.toQueryObject());
        collectionName = q.getCollectionName();
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> matchSubQuery(Query<?> q) {
        Map<String, Object> o = Utils.getMap("$match", q.toQueryObject());
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> limit(int num) {
        Map<String, Object> o = Utils.getMap("$limit", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> skip(int num) {
        Map<String, Object> o = Utils.getMap("$skip", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        Map<String, Object> o = Utils.getMap("$unwind", listField);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> sort(String... prefixed) {
        Map<String, Integer> m = new LinkedHashMap<>();
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
        Map<String, Object> o = Utils.getMap("$sort", sort);
        params.add(o);
        return this;
    }

    @Override
    public String getCollectionName() {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }
        return collectionName;
    }

    @Override
    public void setCollectionName(String cn) {
        collectionName = cn;
    }

    @Override
    public Group<T, R> group(Map<String, Object> id) {
        return new Group<>(this, id);
    }

    @Override
    public Group<T, R> groupSubObj(Map<String, String> idSubObject) {
        //noinspection unchecked,unchecked
        return new Group(this, idSubObject);
    }

    @Override
    public Group<T, R> group(String id) {
        return new Group<>(this, id);
    }

    @Override
    public void addOperator(Map<String, Object> o) {
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

            morphium.queueTask(() -> {
                long start = System.currentTimeMillis();
                List<R> ret = morphium.aggregate(AggregatorImpl.this);
                callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, ret, null, AggregatorImpl.this);
            });
        }
    }

    @Override
    public List<Map<String, Object>> toAggregationList() {
        return params;
    }

}
