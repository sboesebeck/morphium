package de.caluga.morphium.query;

import com.mongodb.*;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
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
    private String where;
    private Class<? extends T> type;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;
    private ReadPreferenceLevel readPreferenceLevel;
    private ReadPreference readPreference;
    private boolean additionalDataPresent = false;

    private int limit = 0, skip = 0;
    private Map<String, Integer> sort;

    private Morphium morphium;
    private static Logger log = Logger.getLogger(Query.class);
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();
    private ThreadPoolExecutor executor;
    private String collectionName;

    public QueryImpl() {

    }

    public QueryImpl(Morphium m, Class<? extends T> type, ThreadPoolExecutor executor) {
        this(m);
        setType(type);
        this.executor = executor;
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
        if (m == null) {
            annotationHelper = new AnnotationAndReflectionHelper();
        } else {
            annotationHelper = m.getARHelper();
        }
        andExpr = new Vector<FilterExpression>();
        orQueries = new Vector<Query<T>>();
        norQueries = new Vector<Query<T>>();
    }


    public QueryImpl(Morphium m) {
        setMorphium(m);
    }

    public ReadPreferenceLevel getReadPreferenceLevel() {
        return readPreferenceLevel;
    }

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel) {
        this.readPreferenceLevel = readPreferenceLevel;
        readPreference = readPreferenceLevel.getPref();
    }

    @Override
    public void setType(Class<? extends T> type) {
        this.type = type;
        DefaultReadPreference pr = annotationHelper.getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            setReadPreferenceLevel(pr.value());
        }
        List<String> fields = annotationHelper.getFields(type, AdditionalData.class);
        additionalDataPresent = fields != null && fields.size() != 0;
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
        String ck = morphium.getCache().getCacheKey(query, sort, getCollectionName(), skip, limit);
        if (morphium.getCache().isCached(type, ck)) {
            return morphium.getCache().getFromCache(type, ck);
        }
        long start = System.currentTimeMillis();
        DBCollection c = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(c);
        List<Field> fldlst = annotationHelper.getAllFields(type);
        BasicDBObject lst = new BasicDBObject();
        lst.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                lst = new BasicDBObject();
                break;
            }
            String n = annotationHelper.getFieldName(type, f.getName());
            lst.put(n, 1);
        }

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
        List<T> ret = new ArrayList<T>();

        while (cursor.hasNext()) {
            ret.add(morphium.getMapper().unmarshall(type, cursor.next()));
        }
        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);
        return ret;
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
    public Map<String, Integer> getSort() {
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
    public MongoField<T> f(String f) {
        String cf = f;
        if (f.contains(".")) {
            //if there is a . only check first part
            cf = f.substring(0, f.indexOf("."));
            //TODO: check field name completely => person.name, check type Person for field name
        } else if (additionalDataPresent) {
            log.debug("Additional data is available, not checking field");
            MongoField<T> fld = morphium.createMongoField(); //new MongoFieldImpl<T>();
            fld.setFieldString(f);
            fld.setMapper(morphium.getMapper());
            fld.setQuery(this);
            return fld;
        }
        Field field = annotationHelper.getField(type, cf);
        if (field == null) {

            throw new IllegalArgumentException("Unknown Field " + f);
        }

        if (field.isAnnotationPresent(Id.class)) {
            f = "_id";
        } else {
            f = annotationHelper.getFieldName(type, f); //handling of aliases
        }
        MongoField<T> fld = morphium.createMongoField(); //new MongoFieldImpl<T>();
        fld.setFieldString(f);
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
    public Query<T> sort(Map<String, Integer> n) {
        sort = n;
        return this;
    }

    @Override
    public Query<T> sort(String... prefixedString) {
        Map<String, Integer> m = new LinkedHashMap<String, java.lang.Integer>();
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
            if (!fld.contains(".")) {
                fld = annotationHelper.getFieldName(type, fld);
            }
            m.put(fld, val);
        }
        return sort(m);
    }

    @Override
    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Integer> m = new LinkedHashMap<String, java.lang.Integer>();
        for (Enum i : naturalOrder) {
            String fld = annotationHelper.getFieldName(type, i.name());
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
        long ret = collection.count(toQueryObject());
        morphium.fireProfilingReadEvent(QueryImpl.this, System.currentTimeMillis() - start, ReadAccessType.COUNT);
        return ret;
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
        Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache();

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

        List<Field> fldlst = annotationHelper.getAllFields(type);
        BasicDBObject lst = new BasicDBObject();
        lst.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                lst = new BasicDBObject();
                break;
            }
            String n = annotationHelper.getFieldName(type, f.getName());
            lst.put(n, 1);
        }

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

        Iterator<DBObject> it = query.iterator();
        List<T> ret = new ArrayList<T>();

        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);
        while (it.hasNext()) {
            DBObject o = it.next();
            T unmarshall = morphium.getMapper().unmarshall(type, o);
            ret.add(unmarshall);

            updateLastAccess(o, unmarshall);

            morphium.firePostLoadEvent(unmarshall);
        }


        if (useCache) {
            morphium.getCache().addToCache(ck, type, ret);
        }
        return ret;
    }

    @Override
    public MorphiumIterator<T> asIterable() {
        return asIterable(10);
    }

    @Override
    public MorphiumIterator<T> asIterable(int windowSize) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("creating iterable for query - windowsize " + windowSize);
            }
            MorphiumIterator<T> it = morphium.getConfig().getIteratorClass().newInstance();
            it.setQuery(this);
            it.setWindowSize(windowSize);
            return it;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateLastAccess(DBObject o, T unmarshall) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, LastAccess.class)) {
            List<String> lst = annotationHelper.getFields(type, LastAccess.class);
            for (String ctf : lst) {
                Field f = annotationHelper.getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(unmarshall, System.currentTimeMillis());
                    } catch (IllegalAccessException e) {
                        System.out.println("Could not set modification time");

                    }
                }
            }

            //Storing access timestamps
            morphium.store(unmarshall);
        }
    }

    @Override
    public void getById(final ObjectId id, final AsyncOperationCallback<T> callback) {
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
    public T getById(ObjectId id) {
        List<String> flds = annotationHelper.getFields(type, Id.class);
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
        Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        String ck = morphium.getCache().getCacheKey(this);
        morphium.inc(StatisticKeys.READS);
        if (readCache) {
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
        List<Field> fldlst = annotationHelper.getAllFields(type);
        BasicDBObject fl = new BasicDBObject();
        fl.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                fl = new BasicDBObject();
                break;
            }
            String n = annotationHelper.getFieldName(type, f.getName());
            fl.put(n, 1);
        }

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

        DBObject ret = srch.toArray(1).get(0);
        List<T> lst = new ArrayList<T>(1);
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.GET);

        if (ret != null) {
            T unmarshall = morphium.getMapper().unmarshall(type, ret);
            morphium.firePostLoadEvent(unmarshall);
            updateLastAccess(ret, unmarshall);

            lst.add((T) unmarshall);
            if (readCache) {
                morphium.getCache().addToCache(ck, type, lst);
            }
            return unmarshall;
        }

        if (readCache) {
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
                    List<ObjectId> ret = idList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, null, null, ret);
                } catch (Exception e) {
                    callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };

        getExecutor().submit(r);
    }

    @Override
    public List<ObjectId> idList() {
        Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class);//type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        List<ObjectId> ret = new ArrayList<ObjectId>();
        String ck = morphium.getCache().getCacheKey(this);
        ck += " idlist";
        morphium.inc(StatisticKeys.READS);
        if (readCache) {

            if (morphium.getCache().isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                //casts are not nice... any idea how to change that?
                return (List<ObjectId>) morphium.getCache().getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(getCollectionName());
        setReadPreferenceFor(collection);
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
            ret.add((ObjectId) o.get("_id"));
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.ID_LIST);
        if (readCache) {
            morphium.getCache().addToCache(ck, type, ret);
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
                ret.sort = new Hashtable<String, Integer>();
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
    public List<T> textSearch(String... texts) {
        return textSearch(TextSearchLanguages.mongo_default, texts);
    }

    @Override
    public List<T> textSearch(TextSearchLanguages lang, String... texts) {
        throw new RuntimeException("not implemented yet");
//        if (texts.length==0) return new ArrayList<T>();
//        BasicDBObject cmd=new BasicDBObject();
//
//        BasicDBObject txt=new BasicDBObject();
//        StringBuilder b=new StringBuilder();
//        for (String t:texts) {
//            b.append("\"");
//            b.append(t);
//            b.append("\" ");
//        }
//        txt.append("search",b.toString());
//        txt.append("filter",toQueryObject());
//        if (getLimit()>0) {
//            txt.append("limit",limit);
//        }
//         if (!lang.equals(TextSearchLanguages.mongo_default)) {
//             txt.append("language",lang.name());
//         }
//        cmd.append("text",txt);
//
//        DBCollection col=getMorphium().getDatabase().getCollection(getCollectionName());
//        Method m= null;
//        try {
//            m = col.getClass().getMethod("command", DBObject.class, int.class, ReadPreference.class);
//        } catch (NoSuchMethodException e) {
//            throw new RuntimeException(e);
//        }
//        m.setAccessible(true);
//        CommandResult result= null;
//        try {
//            result = (CommandResult)m.invoke(col, cmd, col.getOptions(), col.getReadPreference());
//        } catch (IllegalAccessException e) {
//            //TODO: Implement Handling
//            throw new RuntimeException(e);
//        } catch (InvocationTargetException e) {
//            //TODO: Implement Handling
//            throw new RuntimeException(e);
//        }
//        if (!result.ok()) {
//            return null;
//        }
//        BasicDBList lst=(BasicDBList)result.get("results");
//        List<T> ret=new ArrayList<T>();
//        for (Object o:lst) {
//            DBObject obj=(DBObject)o;
//            ret.add(morphium.getMapper().unmarshall(getType(),obj));
//        }
//        return ret;
    }
}
