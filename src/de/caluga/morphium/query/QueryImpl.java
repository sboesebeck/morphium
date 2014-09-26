package de.caluga.morphium.query;

import com.mongodb.*;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 22:14
 * <p/>
 */
public class QueryImpl<T> implements Query<T>, Cloneable {
    private static Logger log = Logger.getLogger(Query.class);
    private String where;
    private Class<? extends T> type;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;
    private ReadPreferenceLevel readPreferenceLevel;
    private ReadPreference readPreference;
    private boolean additionalDataPresent = false;
    private int limit = 0, skip = 0;
    private Map<String, Object> sort;
    private Morphium morphium;
    private ThreadPoolExecutor executor;
    private String collectionName;
    private ServerAddress srv = null;

    private DBObject fieldList;

    private boolean autoValuesEnabled = true;
    private DBObject additionalFields;

    public QueryImpl() {

    }

    public QueryImpl(Morphium m, Class<? extends T> type, ThreadPoolExecutor executor) {
        this(m);
        setType(type);
        this.executor = executor;
    }

    public QueryImpl(Morphium m) {
        setMorphium(m);
    }


    @Override
    public void disableAutoValues() {
        autoValuesEnabled = false;
    }

    @Override
    public void enableAutoValues() {
        autoValuesEnabled = true;
    }

    public boolean isAutoValuesEnabled() {
        return autoValuesEnabled;
    }

    public void setAutoValuesEnabled(boolean autoValuesEnabled) {
        this.autoValuesEnabled = autoValuesEnabled;
    }

    @Override
    public ServerAddress getServer() {
        return srv;
    }

    public ThreadPoolExecutor getExecutor() {
        if (executor == null) {
            executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }
        return executor;
    }

    @Override
    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @Override
    public String getWhere() {
        return where;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;

        andExpr = new Vector<FilterExpression>();
        orQueries = new Vector<Query<T>>();
        norQueries = new Vector<Query<T>>();
    }

    public ReadPreferenceLevel getReadPreferenceLevel() {
        return readPreferenceLevel;
    }

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel) {
        this.readPreferenceLevel = readPreferenceLevel;
        readPreference = readPreferenceLevel.getPref();
    }

    @Override
    public Query<T> q() {
        Query<T> q = new QueryImpl<T>(morphium, type, executor);
        q.setCollectionName(getCollectionName());
        return q;
    }

    public List<T> complexQuery(DBObject query) {
        return complexQuery(query, (String) null, 0, 0);
    }

    @Override
    public List<T> complexQuery(DBObject query, String sort, int skip, int limit) {
        Map<String, Integer> srt = new HashMap<String, Integer>();
        if (sort != null) {
            String[] tok = sort.split(",");
            for (String t : tok) {
                if (t.startsWith("-")) {
                    srt.put(t.substring(1), -1);
                } else if (t.startsWith("+")) {
                    srt.put(t.substring(1), 1);
                } else {
                    srt.put(t, 1);
                }
            }
        }
        return complexQuery(query, srt, skip, limit);
    }

    @Override
    public List<T> complexQuery(DBObject query, Map<String, Integer> sort, int skip, int limit) {
        Cache ca = morphium.getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = ca != null && ca.readCache() && morphium.isReadCacheEnabledForThread();
        String ck = morphium.getCache().getCacheKey(query, sort, getCollectionName(), skip, limit);
        if (useCache && morphium.getCache().isCached(type, ck)) {
            return morphium.getCache().getFromCache(type, ck);
        }

        long start = System.currentTimeMillis();
        DBCollection c = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(c);
        BasicDBObject lst = getFieldListForQuery();

        List<T> ret = new ArrayList<T>();
        int retries = morphium.getConfig().getRetriesOnNetworkError();
        for (int i = 0; i < retries; i++) {

            try {
                DBCursor cursor = c.find(query, lst);
                if (sort != null) {
                    DBObject srt = new BasicDBObject();
                    srt.putAll(sort);
                    cursor.sort(srt);
                }
                if (skip > 0) {
                    cursor.skip(skip);
                }
                if (limit > 0) {
                    cursor.limit(limit);
                }

                while (cursor.hasNext()) {
                    T unmarshall = morphium.getMapper().unmarshall(type, cursor.next());
                    if (unmarshall != null) ret.add(unmarshall);
                }
                srv = cursor.getServerAddress();
                break;
            } catch (RuntimeException e) {
                morphium.handleNetworkError(i, e);
            }
        }
        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);
        if (useCache) {
            morphium.getCache().addToCache(ck, type, ret);
        }
        return ret;
    }

    private BasicDBObject getFieldListForQuery() {
        List<Field> fldlst = morphium.getARHelper().getAllFields(type);
        BasicDBObject lst = new BasicDBObject();
        lst.put("_id", 1);
        Entity e = morphium.getARHelper().getAnnotationFromHierarchy(type, Entity.class);
        if (e.polymorph()) {
            lst.put("class_name", 1);
            lst.put("type_id", 1);
        }

        if (fieldList != null) {
            lst.putAll(fieldList);
        } else {
            for (Field f : fldlst) {
                if (f.isAnnotationPresent(AdditionalData.class)) {
                    //to enable additional data
                    lst = new BasicDBObject();
                    break;
                }
                String n = morphium.getARHelper().getFieldName(type, f.getName());
                lst.put(n, 1);
            }
        }
        if (additionalFields != null) {
            lst.putAll(additionalFields);
        }
        return lst;
    }

    @Override
    public List distinct(String field) {
        return morphium.getDatabase().getCollection(getCollectionName()).distinct(field, toQueryObject());
    }

    @Override
    public T complexQueryOne(DBObject query) {
        return complexQueryOne(query, null, 0);
    }

    @Override
    public T complexQueryOne(DBObject query, Map<String, Integer> sort, int skip) {
        List<T> ret = complexQuery(query, sort, skip, 1);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }
        return null;
    }

    @Override
    public T complexQueryOne(DBObject query, Map<String, Integer> sort) {
        return complexQueryOne(query, sort, 0);
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public int getSkip() {
        return skip;
    }

    @Override
    public Map<String, Object> getSort() {
        return sort;
    }

    @Override
    public void addChild(FilterExpression ex) {
        andExpr.add(ex);
    }

    @Override
    public Query<T> where(String wh) {
        where = wh;
        return this;
    }

    public MongoField<T> f(Enum f) {
        return f(f.name());
    }

    @Override
    public MongoField<T> f(String... f) {
        StringBuffer b = new StringBuffer();
        for (String e : f) {
            b.append(e);
            b.append(".");
        }
        b.deleteCharAt(b.length());
        return f(b.toString());
    }

    @Override
    public MongoField<T> f(Enum... f) {
        StringBuffer b = new StringBuffer();
        for (Enum e : f) {
            b.append(e.name());
            b.append(".");
        }
        b.deleteCharAt(b.length());
        return f(b.toString());
    }

    public MongoField<T> f(String f) {
        StringBuffer fieldPath = new StringBuffer();
        String cf = f;
        Class<?> clz = type;
        if (f.contains(".")) {
            String[] fieldNames = f.split("\\.");
            for (String fieldName : fieldNames) {
                String fieldNameInstance = morphium.getARHelper().getFieldName(clz, fieldName);
                Field field = morphium.getARHelper().getField(clz, fieldNameInstance);
                if (field == null) {
                    throw new IllegalArgumentException("Field " + fieldNameInstance + " not found!");
                }
                if (field.isAnnotationPresent(Reference.class)) {
                    //cannot join
                    throw new IllegalArgumentException("cannot subquery references: " + fieldNameInstance + " of type " + clz.getName() + " has @Reference");
                }
                fieldPath.append(fieldNameInstance);
                fieldPath.append('.');
                clz = field.getType();
                if (clz.equals(List.class) || clz.equals(Collection.class) || clz.equals(Array.class) || clz.equals(Set.class) || clz.equals(Map.class)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Cannot check fields in generic lists or maps");
                    }
                    clz = Object.class;
                }
                if (clz.equals(Object.class)) {
                    break;
                }
            }
            if (clz.equals(Object.class)) {
                cf = f;
            } else {
                cf = fieldPath.substring(0, fieldPath.length() - 1);
            }
        } else {
            cf = morphium.getARHelper().getFieldName(clz, f);
        }
        if (additionalDataPresent) {
            log.debug("Additional data is available, not checking field");
        }
        MongoField<T> fld = morphium.createMongoField();
        fld.setFieldString(cf);
        fld.setMapper(morphium.getMapper());
        fld.setQuery(this);
        return fld;
    }

    @Override
    public Query<T> or(Query<T>... qs) {
        orQueries.addAll(Arrays.asList(qs));
        return this;
    }

    @Override
    public Query<T> or(List<Query<T>> qs) {
        orQueries.addAll(qs);
        return this;
    }

    private Query<T> getClone() {
        try {
            return clone();
        } catch (CloneNotSupportedException e) {
            log.error("Clone not supported?!?!?!");
            throw new RuntimeException(e);
        }
    }

    @Override
    public Query<T> nor(Query<T>... qs) {
        norQueries.addAll(Arrays.asList(qs));
        return this;
    }

    @Override
    public Query<T> limit(int i) {
        limit = i;
        return this;
    }

    @Override
    public Query<T> skip(int i) {
        skip = i;
        return this;
    }

    /**
     * this does not check for existence of the Field! Key in the map can be any text
     *
     * @param n
     * @return
     */
    @Override
    public Query<T> sort(Map<String, Object> n) {
        sort = n;
        return this;
    }

    @Override
    public Query<T> sort(String... prefixedString) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (String i : prefixedString) {
            String fld = i;
            int val = 1;
            if (i.startsWith("-")) {
                fld = i.substring(1);
                val = -1;
            } else if (i.startsWith("+")) {
                fld = i.substring(1);
                val = 1;
            }
            if (!fld.contains(".") && !fld.startsWith("$")) {
                fld = morphium.getARHelper().getFieldName(type, fld);
            }
            m.put(fld, val);
        }
        return sort(m);
    }

    @Override
    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Enum i : naturalOrder) {
            String fld = morphium.getARHelper().getFieldName(type, i.name());
            m.put(fld, 1);
        }
        return sort(m);
    }

    @Override
    public void countAll(final AsyncOperationCallback<T> c) {
        if (c == null) {
            throw new IllegalArgumentException("Not really useful to read from db and not use the result");
        }
        Runnable r = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    long ret = countAll();
                    c.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, null, null, ret);
                } catch (Exception e) {
                    c.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };

        getExecutor().submit(r);

    }

    @Override
    public long countAll() {
        morphium.inc(StatisticKeys.READS);
        long start = System.currentTimeMillis();

        DBCollection collection = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(collection);
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                DBCursor cu = collection.find(toQueryObject());
                long ret = cu.count();
                srv = cu.getServerAddress();
                morphium.fireProfilingReadEvent(QueryImpl.this, System.currentTimeMillis() - start, ReadAccessType.COUNT);
                return ret;
            } catch (RuntimeException e) {
                morphium.handleNetworkError(i, e);
            }
        }
        return 0;
    }

    private void setReadPreferenceFor(DBCollection c) {
        if (readPreference != null) {
            c.setReadPreference(readPreference);
        } else {
            c.setReadPreference(null);
        }
    }

    /**
     * retrun mongo's readPreference
     *
     * @return
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
        readPreferenceLevel = null;
    }

    @Override
    public DBObject toQueryObject() {
        BasicDBObject o = new BasicDBObject();
        BasicDBList lst = new BasicDBList();
        boolean onlyAnd = orQueries.isEmpty() && norQueries.isEmpty() && where == null;
        if (where != null) {
            o.put("$where", where);
        }
        if (andExpr.size() == 1 && onlyAnd) {
            return andExpr.get(0).dbObject();
        }
        if (andExpr.size() == 1 && onlyAnd) {
            return andExpr.get(0).dbObject();
        }

        if (andExpr.isEmpty() && onlyAnd) {
            return o;
        }

        if (andExpr.size() > 0) {
            for (FilterExpression ex : andExpr) {
                lst.add(ex.dbObject());
            }

            o.put("$and", lst);
            lst = new BasicDBList();
        }
        if (orQueries.size() != 0) {
            for (Query<T> ex : orQueries) {
                lst.add(ex.toQueryObject());
            }
            if (o.get("$and") != null) {
                ((BasicDBList) o.get("$and")).add(new BasicDBObject("$or", lst));
            } else {
                o.put("$or", lst);
            }
        }

        if (norQueries.size() != 0) {
            for (Query<T> ex : norQueries) {
                lst.add(ex.toQueryObject());
            }
            if (o.get("$and") != null) {
                ((BasicDBList) o.get("$and")).add(new BasicDBObject("$nor", lst));
            } else {
                o.put("$nor", lst);
            }
        }


        return o;
    }

    @Override
    public Class<? extends T> getType() {
        return type;
    }

    @Override
    public void setType(Class<? extends T> type) {
        this.type = type;
        DefaultReadPreference pr = morphium.getARHelper().getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            setReadPreferenceLevel(pr.value());
        }
        List<String> fields = morphium.getARHelper().getFields(type, AdditionalData.class);
        additionalDataPresent = fields != null && fields.size() != 0;
    }

    @Override
    public void asList(final AsyncOperationCallback<T> callback) {
        if (callback == null) throw new IllegalArgumentException("callback is null");
        Runnable r = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    List<T> lst = asList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, lst, null);
                } catch (Exception e) {
                    callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        getExecutor().submit(r);
    }

    @Override
    public List<T> asList() {
        morphium.inc(StatisticKeys.READS);
        Cache c = morphium.getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();

        String ck = morphium.getCache().getCacheKey(this);
        if (useCache) {
            if (morphium.getCache().isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                return morphium.getCache().getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);

        }
        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(collection);
        BasicDBObject lst = getFieldListForQuery();


        List<T> ret = new ArrayList<T>();
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            ret.clear();
            try {
                DBCursor query = collection.find(toQueryObject(), lst);
                if (skip > 0) {
                    query.skip(skip);
                }
                if (limit > 0) {
                    query.limit(limit);
                }
                if (sort != null) {
                    BasicDBObject srt = new BasicDBObject();
                    for (String k : sort.keySet()) {
                        srt.append(k, sort.get(k));
                    }
                    query.sort(new BasicDBObject(srt));
                }
                srv = query.getServerAddress();
                Iterator<DBObject> it = query.iterator();


                while (it.hasNext()) {
                    DBObject o = it.next();
                    T unmarshall = morphium.getMapper().unmarshall(type, o);
                    if (unmarshall != null) {
                        ret.add(unmarshall);
                        updateLastAccess(unmarshall);
                        morphium.firePostLoadEvent(unmarshall);
                    }


                }
                break;

            } catch (Throwable es) {
                morphium.handleNetworkError(i, es);
            }
        }
        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);

        if (useCache) {
            morphium.getCache().addToCache(ck, type, ret);
        }
        morphium.firePostLoad(ret);
        return ret;
    }

    @Override
    public MorphiumIterator<T> asIterable() {
        return asIterable(10, 1);
    }

    public MorphiumIterator<T> asIterable(int windowSize) {
        return asIterable(windowSize, 1);
    }

    @Override
    public MorphiumIterator<T> asIterable(int windowSize, int prefixWindows) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("creating iterable for query - windowsize " + windowSize);
            }
            MorphiumIterator<T> it = morphium.getConfig().getIteratorClass().newInstance();
            it.setQuery(this);
            it.setWindowSize(windowSize);
            it.setNumberOfPrefetchWindows(prefixWindows);
            return it;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateLastAccess(T unmarshall) {
        if (!autoValuesEnabled) return;
        if (!morphium.isAutoValuesEnabledForThread()) return;
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, LastAccess.class)) {
            List<String> lst = morphium.getARHelper().getFields(type, LastAccess.class);
            for (String ctf : lst) {
                Field f = morphium.getARHelper().getField(type, ctf);
                if (f != null) {
                    try {
                        long currentTime = System.currentTimeMillis();
                        if (f.getType().equals(Date.class)) {
                            f.set(unmarshall, new Date());
                        } else if (f.getType().equals(String.class)) {
                            LastAccess ctField = f.getAnnotation(LastAccess.class);
                            SimpleDateFormat df = new SimpleDateFormat(ctField.dateFormat());
                            f.set(unmarshall, df.format(currentTime));
                        } else {
                            f.set(unmarshall, currentTime);

                        }
                        ObjectMapper mapper = morphium.getMapper();
                        String collName = mapper.getCollectionName(unmarshall.getClass());
                        Object id = morphium.getARHelper().getId(unmarshall);
                        //Cannot use store, as this would trigger an update of last changed...
                        morphium.getDatabase().getCollection(collName).update(new BasicDBObject("_id", id), new BasicDBObject("$set", new BasicDBObject(ctf, currentTime)));
                    } catch (IllegalAccessException e) {
                        System.out.println("Could not set modification time");

                    }
                }
            }

            //Storing access timestamps
//            List<T> l=new ArrayList<T>();
//            l.add(unmarshall);
//            morphium.getWriterForClass(unmarshall.getClass()).store(l,null);

//            morphium.store(unmarshall);
        }
    }

    @Override
    public void getById(final Object id, final AsyncOperationCallback<T> callback) {
        if (callback == null) throw new IllegalArgumentException("Callback is null");
        Runnable c = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    T res = getById(id);
                    List<T> result = new ArrayList<T>();
                    result.add(res);
                    callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, result, res);
                } catch (Exception e) {
                    callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        getExecutor().submit(c);
    }

    @Override
    public T getById(Object id) {
        List<String> flds = morphium.getARHelper().getFields(type, Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new RuntimeException("Type does not have an ID-Field? " + type.getSimpleName());
        }
        //should only be one
        String f = flds.get(0);
        Query<T> q = q().f(f).eq(id); //prepare
        return q.get();
    }

    @Override
    public void get(final AsyncOperationCallback<T> callback) {
        if (callback == null) throw new IllegalArgumentException("Callback is null");
        Runnable r = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    List<T> ret = new ArrayList<T>();
                    T ent = get();
                    ret.add(ent);
                    callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, ret, ent);
                } catch (Exception e) {
                    callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        getExecutor().submit(r);
    }

    @Override
    public T get() {
        Cache c = morphium.getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();
        String ck = morphium.getCache().getCacheKey(this);
        morphium.inc(StatisticKeys.READS);
        if (useCache) {
            if (morphium.getCache().isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                List<T> lst = morphium.getCache().getFromCache(type, ck);
                if (lst == null || lst.isEmpty()) {
                    return null;
                } else {
                    return lst.get(0);
                }

            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();
        DBCollection coll = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(coll);
        BasicDBObject fl = getFieldListForQuery();

        DBCursor srch = coll.find(toQueryObject(), fl);
        srch.limit(1);
        if (skip != 0) {
            srch = srch.skip(skip);
        }
        if (sort != null) {
            BasicDBObject srt = new BasicDBObject();
            for (String k : sort.keySet()) {
                srt.append(k, sort.get(k));
            }
            srch.sort(new BasicDBObject(srt));
        }

        if (srch.length() == 0) {
            return null;
        }

        DBObject ret = null;
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                ret = srch.toArray(1).get(0);
                srv = srch.getServerAddress();
                break;
            } catch (RuntimeException e) {
                morphium.handleNetworkError(i, e);
            }
        }
        List<T> lst = new ArrayList<T>(1);
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.GET);

        if (ret != null) {
            T unmarshall = morphium.getMapper().unmarshall(type, ret);
            if (unmarshall != null) {
                morphium.firePostLoadEvent(unmarshall);
                updateLastAccess(unmarshall);

                lst.add((T) unmarshall);
                if (useCache) {
                    morphium.getCache().addToCache(ck, type, lst);
                }
            }
            return unmarshall;
        }

        if (useCache) {
            morphium.getCache().addToCache(ck, type, lst);
        }
        return null;
    }

    @Override
    public void idList(final AsyncOperationCallback<T> callback) {
        if (callback == null) throw new IllegalArgumentException("Callable is null?");
        Runnable r = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    List<Object> ret = idList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, null, null, ret);
                } catch (Exception e) {
                    callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };

        getExecutor().submit(r);
    }

    @Override
    public <R> List<R> idList() {
        Cache c = morphium.getARHelper().getAnnotationFromHierarchy(type, Cache.class);//type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();
        List<R> ret = new ArrayList<R>();
        String ck = morphium.getCache().getCacheKey(this);
        ck += " idlist";
        morphium.inc(StatisticKeys.READS);
        if (useCache) {

            if (morphium.getCache().isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                //casts are not nice... any idea how to change that?
                return (List<R>) morphium.getCache().getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(collection);
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                DBCursor query = collection.find(toQueryObject(), new BasicDBObject("_id", 1)); //only get IDs
                if (sort != null) {
                    query.sort(new BasicDBObject(sort));
                }
                if (skip > 0) {
                    query.skip(skip);
                }
                if (limit > 0) {
                    query.limit(0);
                }

                for (DBObject o : query) {
                    ret.add((R) o.get("_id"));
                }
                srv = query.getServerAddress();
                break;
            } catch (RuntimeException e) {
                morphium.handleNetworkError(i, e);
            }
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.ID_LIST);
        if (useCache) {
            morphium.getCache().addToCache(ck, (Class<? extends R>) type, ret);
        }
        return ret;
    }

    public Query<T> clone() throws CloneNotSupportedException {
        try {
            QueryImpl<T> ret = (QueryImpl<T>) super.clone();
            if (andExpr != null) {
                ret.andExpr = new Vector<FilterExpression>();
                ret.andExpr.addAll(andExpr);
            }
            if (norQueries != null) {
                ret.norQueries = new Vector<Query<T>>();
                ret.norQueries.addAll(norQueries);
            }
            if (sort != null) {
                ret.sort = new Hashtable<String, Object>();
                ret.sort.putAll(sort);
            }
            if (orQueries != null) {
                ret.orQueries = new Vector<Query<T>>();
                ret.orQueries.addAll(orQueries);
            }
            if (readPreferenceLevel != null) {
                ret.readPreferenceLevel = readPreferenceLevel;
            }
            if (readPreference != null) {
                ret.readPreference = readPreference;
            }
            if (where != null) {
                ret.where = where;
            }


            return ret;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete() {
        morphium.delete(this);
    }

    @Override
    public int getNumberOfPendingRequests() {
        return getExecutor().getActiveCount();
    }

    @Override
    public String getCollectionName() {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }
        return collectionName;
    }

    @Override
    public void setCollectionName(String n) {
        collectionName = n;
    }

    @Override
    public Query<T> text(String... text) {
        return text(null, (TextSearchLanguages) null, text);
    }

    @Override
    public Query<T> text(TextSearchLanguages lang, String... text) {
        return text(null, lang, text);
    }

    @Override
    public Query<T> text(String metaScoreField, TextSearchLanguages lang, String... text) {
        FilterExpression f = new FilterExpression();
        f.setField("$text");
        StringBuilder b = new StringBuilder();
        for (String t : text) {
            b.append(t);
            b.append(" ");
        }
        f.setValue(new BasicDBObject("$search", b.toString()));
        if (lang != null) {
            ((BasicDBObject) f.getValue()).put("$language", lang.toString());
        }
        addChild(f);
        if (metaScoreField != null) {

            additionalFields = new BasicDBObject(metaScoreField, new BasicDBObject(new BasicDBObject("$meta", "textScore")));

        }

        return this;

    }

    @Override
    @Deprecated
    public List<T> textSearch(String... texts) {
        return textSearch(TextSearchLanguages.mongo_default, texts);
    }

    @Override
    @Deprecated
    public List<T> textSearch(TextSearchLanguages lang, String... texts) {
        if (texts.length == 0) return new ArrayList<T>();

        BasicDBObject txt = new BasicDBObject();
        txt.append("text", getCollectionName());
        StringBuilder b = new StringBuilder();
        for (String t : texts) {
//            b.append("\"");
            b.append(t);
            b.append(" ");
//            b.append("\" ");
        }
        txt.append("search", b.toString());
        txt.append("filter", toQueryObject());
        if (getLimit() > 0) {
            txt.append("limit", limit);
        }
        if (!lang.equals(TextSearchLanguages.mongo_default)) {
            txt.append("language", lang.name());
        }

        CommandResult result = morphium.getDatabase().command(txt);


        if (!result.ok()) {
            return null;
        }
        BasicDBList lst = (BasicDBList) result.get("results");
        List<T> ret = new ArrayList<T>();
        for (Object o : lst) {
            DBObject obj = (DBObject) o;
            T unmarshall = morphium.getMapper().unmarshall(getType(), obj);
            if (unmarshall != null) ret.add(unmarshall);
        }
        return ret;
    }

    @Override
    public void setReturnedFields(Enum... fl) {
        for (Enum f : fl) {
            addReturnedField(f);
        }
    }

    @Override
    public void setReturnedFields(String... fl) {
        fieldList = new BasicDBObject();
        for (String f : fl) {
            addReturnedField(f);
        }
    }


    @Override
    public void addReturnedField(Enum f) {
        addReturnedField(f.name());
    }

    @Override
    public void addReturnedField(String f) {
        if (fieldList == null) {
            fieldList = new BasicDBObject();
        }
        String n = morphium.getARHelper().getFieldName(type, f);
        fieldList.put(n, 1);
    }

    @Override
    public String toString() {
        StringBuilder and = new StringBuilder();
        if (andExpr != null && andExpr.size() > 0) {
            and.append("[");
            for (FilterExpression fe : andExpr) {
                and.append(fe.toString());
                and.append(", ");
            }
            and.deleteCharAt(and.length() - 1);
            and.deleteCharAt(and.length() - 1);
            and.append(" ]");
        }

        StringBuilder ors = new StringBuilder();
        if (orQueries != null && orQueries.size() > 0) {
            ors.append("[ ");
            for (Query<T> o : orQueries) {
                ors.append(o.toString());
                ors.append(", ");
            }
            ors.deleteCharAt(ors.length() - 1);
            ors.deleteCharAt(ors.length() - 1);
            ors.append(" ]");
        }

        StringBuilder nors = new StringBuilder();
        if (norQueries != null && norQueries.size() > 0) {
            nors.append("[ ");
            for (Query<T> o : norQueries) {
                nors.append(o.toString());
                nors.append(", ");
            }
            nors.deleteCharAt(nors.length() - 1);
            nors.deleteCharAt(nors.length() - 1);
            nors.append(" ]");
        }

        String ret = "Query{ " +
                "collectionName='" + collectionName + '\'' +
                ", type=" + type.getName() +
                ", skip=" + skip +
                ", limit=" + limit +
                ", andExpr=" + and.toString() +
                ", orQueries=" + ors +
                ", norQueries=" + nors +
                ", sort=" + sort +
                ", readPreferenceLevel=" + readPreferenceLevel +
                ", additionalDataPresent=" + additionalDataPresent +
                ", where='" + where + '\'' +
                '}';
        if (fieldList != null) {
            ret += " Fields " + fieldList.toString();

        }
        return ret;
    }
}
