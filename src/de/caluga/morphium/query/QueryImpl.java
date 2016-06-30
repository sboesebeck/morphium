package de.caluga.morphium.query;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 22:14
 * <p>
 */
@SuppressWarnings("WeakerAccess")
public class QueryImpl<T> implements Query<T>, Cloneable {
    private static final Logger log = new Logger(Query.class);
    private String where;
    private Class<? extends T> type;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;
    private ReadPreferenceLevel readPreferenceLevel;
    private boolean additionalDataPresent = false;
    private int limit = 0, skip = 0;
    private Map<String, Integer> sort;
    private Morphium morphium;
    private ThreadPoolExecutor executor;
    private String collectionName;
    private String srv = null;

    private Map<String, Object> fieldList;

    private boolean autoValuesEnabled = true;
    private Map<String, Object> additionalFields;

    private String tags;
    private AnnotationAndReflectionHelper arHelper;

    public QueryImpl() {

    }

    public QueryImpl(Morphium m, Class<? extends T> type, ThreadPoolExecutor executor) {
        this(m);
        setType(type);
        this.executor = executor;
        if (m.getConfig().getDefaultTagSet() != null) {
            tags = m.getConfig().getDefaultTags();
        }
    }

    public QueryImpl(Morphium m) {
        setMorphium(m);
    }


    @Override
    public String[] getTags() {
        if (tags == null) {
            return new String[0];
        }
        return tags.split(",");
    }

    @Override
    public Query<T> addTag(String name, String value) {
        if (tags != null) {
            tags += ",";
        } else {
            tags = "";
        }
        tags += name + ":" + value;
        return this;
    }


    @Override
    public Query<T> disableAutoValues() {
        autoValuesEnabled = false;
        return this;
    }

    @Override
    public Query<T> enableAutoValues() {
        autoValuesEnabled = true;
        return this;
    }

    public boolean isAutoValuesEnabled() {
        return autoValuesEnabled;
    }

    public Query<T> setAutoValuesEnabled(boolean autoValuesEnabled) {
        this.autoValuesEnabled = autoValuesEnabled;
        return this;
    }

    @Override
    public String getServer() {
        return srv;
    }

    public ThreadPoolExecutor getExecutor() {
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
        setARHelpter(m.getARHelper());
        andExpr = new ArrayList<>();
        orQueries = new ArrayList<>();
        norQueries = new ArrayList<>();
    }

    public ReadPreferenceLevel getReadPreferenceLevel() {
        return readPreferenceLevel;
    }

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel) {
        this.readPreferenceLevel = readPreferenceLevel;
    }

    @Override
    public Query<T> q() {
        Query<T> q = new QueryImpl<>(morphium, type, executor);
        q.setCollectionName(getCollectionName());
        return q;
    }

    public List<T> complexQuery(Map<String, Object> query) {
        return complexQuery(query, (String) null, 0, 0);
    }

    @Override
    public List<T> complexQuery(Map<String, Object> query, String sort, int skip, int limit) {
        Map<String, Integer> srt = new HashMap<>();
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
    public AnnotationAndReflectionHelper getARHelper() {
        if (arHelper == null) {
            arHelper = morphium.getARHelper();
        }
        return arHelper;
    }

    @Override
    public void setARHelpter(AnnotationAndReflectionHelper ar) {
        arHelper = ar;
    }


    @Override
    public List<T> complexQuery(Map<String, Object> query, Map<String, Integer> sort, int skip, int limit) {
        Cache ca = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = ca != null && ca.readCache() && morphium.isReadCacheEnabledForThread();
        String ck = morphium.getCache().getCacheKey(query, sort, getCollectionName(), skip, limit);
        if (useCache && morphium.getCache().isCached(type, ck)) {
            return morphium.getCache().getFromCache(type, ck);
        }

        long start = System.currentTimeMillis();
        Map<String, Object> lst = getFieldListForQuery();

        List<T> ret = new ArrayList<>();

        List<Map<String, Object>> obj;
        Map<String, Object> findMetaData = new HashMap<>();
        try {
            obj = morphium.getDriver().find(morphium.getConfig().getDatabase(), getCollectionName(), query, sort, lst, skip, limit, 100, getRP(), findMetaData);
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
        for (Map<String, Object> in : obj) {
            T unmarshall = morphium.getMapper().unmarshall(type, in);
            if (unmarshall != null) {
                ret.add(unmarshall);
            }
        }
        srv = (String) findMetaData.get("server");
        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);
        if (useCache) {
            morphium.getCache().addToCache(ck, type, ret);
        }
        return ret;
    }

    @Override
    public Map<String, Object> getFieldListForQuery() {
        List<Field> fldlst = getARHelper().getAllFields(type);
        Map<String, Object> lst = new HashMap<>();
        lst.put("_id", 1);
        Entity e = getARHelper().getAnnotationFromHierarchy(type, Entity.class);
        if (e.polymorph()) {
            //            lst.put("class_name", 1);
            return new HashMap<>();
        }

        if (fieldList != null) {
            lst.putAll(fieldList);
        } else {
            for (Field f : fldlst) {
                if (f.isAnnotationPresent(AdditionalData.class)) {
                    //to enable additional data
                    lst = new HashMap<>();
                    break;
                }
                String n = getARHelper().getFieldName(type, f.getName());
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
        try {
            return morphium.getDriver().distinct(morphium.getConfig().getDatabase(), getCollectionName(), field, toQueryObject(), morphium.getReadPreferenceForClass(getType()));
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
    }

    @Override
    public T complexQueryOne(Map<String, Object> query) {
        return complexQueryOne(query, null, 0);
    }

    @Override
    public T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort, int skip) {
        List<T> ret = complexQuery(query, sort, skip, 1);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }
        return null;
    }

    @Override
    public T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort) {
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
    public Query<T> addChild(FilterExpression ex) {
        andExpr.add(ex);
        return this;
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
        StringBuilder b = new StringBuilder();
        for (String e : f) {
            b.append(e);
            b.append(".");
        }
        b.deleteCharAt(b.length());
        return f(b.toString());
    }

    @Override
    public MongoField<T> f(Enum... f) {
        StringBuilder b = new StringBuilder();
        for (Enum e : f) {
            b.append(e.name());
            b.append(".");
        }
        b.deleteCharAt(b.length());
        return f(b.toString());
    }

    public MongoField<T> f(String f) {
        StringBuilder fieldPath = new StringBuilder();
        String cf;
        Class<?> clz = type;
        if (f.contains(".")) {
            String[] fieldNames = f.split("\\.");
            for (String fieldName : fieldNames) {
                String fieldNameInstance = getARHelper().getFieldName(clz, fieldName);
                Field field = getARHelper().getField(clz, fieldNameInstance);
                if (field == null) {
                    throw new IllegalArgumentException("Field " + fieldNameInstance + " not found!");
                }
                //                if (field.isAnnotationPresent(Reference.class)) {
                //                    //cannot join
                //                    throw new IllegalArgumentException("cannot subquery references: " + fieldNameInstance + " of type " + clz.getName() + " has @Reference");
                //                }
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
            cf = getARHelper().getFieldName(clz, f);

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

    @SafeVarargs
    @Override
    public final Query<T> or(Query<T>... qs) {
        orQueries.addAll(Arrays.asList(qs));
        return this;
    }

    @Override
    public Query<T> or(List<Query<T>> qs) {
        orQueries.addAll(qs);
        return this;
    }

    @SuppressWarnings("unused")
    private Query<T> getClone() {
        try {
            return clone();
        } catch (CloneNotSupportedException e) {
            log.error("Clone not supported?!?!?!");
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @Override
    public final Query<T> nor(Query<T>... qs) {
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
        Map<String, Integer> m = new LinkedHashMap<>();
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
                fld = getARHelper().getFieldName(type, fld);
            }
            m.put(fld, val);
        }
        return sort(m);
    }

    @Override
    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Integer> m = new LinkedHashMap<>();
        for (Enum i : naturalOrder) {
            String fld = getARHelper().getFieldName(type, i.name());
            m.put(fld, 1);
        }
        return sort(m);
    }

    @Override
    public void countAll(final AsyncOperationCallback<T> c) {
        if (c == null) {
            throw new IllegalArgumentException("Not really useful to read from db and not use the result");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                long ret = countAll();
                c.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, null, null, ret);
            } catch (Exception e) {
                c.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };

        getExecutor().submit(r);

    }

    @Override
    public long countAll() {
        morphium.inc(StatisticKeys.READS);
        long start = System.currentTimeMillis();
        long ret;
        try {
            ret = morphium.getDriver().count(morphium.getConfig().getDatabase(), getCollectionName(), toQueryObject(), getRP());
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }

        morphium.fireProfilingReadEvent(QueryImpl.this, System.currentTimeMillis() - start, ReadAccessType.COUNT);
        return ret;
    }


    private de.caluga.morphium.driver.ReadPreference getRP() {
        if (readPreferenceLevel == null) {
            return null;
        }
        switch (readPreferenceLevel) {
            case PRIMARY:
                return de.caluga.morphium.driver.ReadPreference.primary();
            case PRIMARY_PREFERRED:
                return de.caluga.morphium.driver.ReadPreference.primaryPreferred();
            case SECONDARY:
                return de.caluga.morphium.driver.ReadPreference.secondary();
            case SECONDARY_PREFERRED:
                return de.caluga.morphium.driver.ReadPreference.secondaryPreferred();
            case NEAREST:
                return de.caluga.morphium.driver.ReadPreference.nearest();
            default:
                return null;
        }
    }

    @Override
    public Map<String, Object> toQueryObject() {
        Map<String, Object> o = new HashMap<>();
        List<Map<String, Object>> lst = new ArrayList<>();
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

        if (!andExpr.isEmpty()) {
            for (FilterExpression ex : andExpr) {
                lst.add(ex.dbObject());
            }

            o.put("$and", lst);
            lst = new ArrayList<>();
        }
        if (!orQueries.isEmpty()) {
            for (Query<T> ex : orQueries) {
                lst.add(ex.toQueryObject());
            }
            if (o.get("$and") != null) {
                //noinspection unchecked
                ((List<Map<String, Object>>) o.get("$and")).
                        add(Utils.getMap("$or", lst));
            } else {
                o.put("$or", lst);
            }
        }

        if (!norQueries.isEmpty()) {
            for (Query<T> ex : norQueries) {
                lst.add(ex.toQueryObject());
            }
            if (o.get("$and") != null) {
                //noinspection unchecked
                ((List<Map<String, Object>>) o.get("$and")).
                        add(Utils.getMap("$nor", lst));
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
        DefaultReadPreference pr = getARHelper().getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            setReadPreferenceLevel(pr.value());
        }
        @SuppressWarnings("unchecked") List<String> fields = getARHelper().getFields(type, AdditionalData.class);
        additionalDataPresent = fields != null && !fields.isEmpty();
    }

    @Override
    public void asList(final AsyncOperationCallback<T> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("callback is null");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                List<T> lst = asList();
                callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, lst, null);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(r);
    }

    @Override
    public List<T> asList() {
        morphium.inc(StatisticKeys.READS);
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
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

        Map<String, Object> lst = getFieldListForQuery();


        List<T> ret = new ArrayList<>();
        ret.clear();
        try {

            Map<String, Object> findMetaData = new HashMap<>();
            List<Map<String, Object>> query = morphium.getDriver().find(morphium.getConfig().getDatabase(), getCollectionName(), toQueryObject(), sort, lst, skip, limit, morphium.getConfig().getCursorBatchSize(), getRP(), findMetaData);
            srv = (String) findMetaData.get("server");


            for (Map<String, Object> o : query) {
                T unmarshall = morphium.getMapper().unmarshall(type, o);
                if (unmarshall != null) {
                    ret.add(unmarshall);
                    updateLastAccess(unmarshall);
                    morphium.firePostLoadEvent(unmarshall);
                }


            }

        } catch (Exception e) {
            throw new RuntimeException(e);

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
        MorphiumDriverIterator<T> it = new MorphiumDriverIterator<>();
        it.setQuery(this);
        return it;
    }

    @Override
    public MorphiumIterator<T> asIterable(int windowSize, Class<? extends MorphiumIterator<T>> it) {
        try {
            MorphiumIterator<T> ret = it.newInstance();
            return asIterable(windowSize, ret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MorphiumIterator<T> asIterable(int windowSize, MorphiumIterator<T> ret) {
        try {
            ret.setQuery(this);
            ret.setWindowSize(windowSize);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MorphiumIterator<T> asIterable(int windowSize) {
        if (log.isDebugEnabled()) {
            log.debug("creating iterable for query - windowsize " + windowSize);
        }
        MorphiumIterator<T> it = new MorphiumDriverIterator<>();
        it.setQuery(this);
        it.setWindowSize(windowSize);
        return it;
    }


    @Override
    public MorphiumIterator<T> asIterable(int windowSize, int prefixWindows) {

        if (log.isDebugEnabled()) {
            log.debug("creating iterable for query - windowsize " + windowSize);
        }
        MorphiumIterator<T> it;
        if (prefixWindows == 1) {
            it = new DefaultMorphiumIterator<>();
        } else {
            it = new PrefetchingDriverIterator<>();
            it.setNumberOfPrefetchWindows(prefixWindows);
        }
        it.setQuery(this);
        it.setWindowSize(windowSize);
        return it;
    }

    private void updateLastAccess(T unmarshall) {
        if (!autoValuesEnabled) {
            return;
        }
        if (!morphium.isAutoValuesEnabledForThread()) {
            return;
        }
        if (getARHelper().isAnnotationPresentInHierarchy(type, LastAccess.class)) {
            @SuppressWarnings("unchecked") List<String> lst = getARHelper().getFields(type, LastAccess.class);
            for (String ctf : lst) {
                Field f = getARHelper().getField(type, ctf);
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
                        Object id = getARHelper().getId(unmarshall);
                        //Cannot use store, as this would trigger an update of last changed...
                        morphium.getDriver().update(morphium.getConfig().getDatabase(), getCollectionName(), Utils.getMap("_id", id), Utils.getMap("$set", Utils.getMap(ctf, currentTime)), false, false, null);
                        //                        morphium.getDatabase().getCollection(collName).update(new HashMap<String, Object>("_id", id), new HashMap<String, Object>("$set", new HashMap<String, Object>(ctf, currentTime)));
                    } catch (Exception e) {
                        log.error("Could not set modification time");
                        throw new RuntimeException(e);
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
        if (callback == null) {
            throw new IllegalArgumentException("Callback is null");
        }
        Runnable c = () -> {
            long start = System.currentTimeMillis();
            try {
                T res = getById(id);
                List<T> result = new ArrayList<>();
                result.add(res);
                callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, result, res);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(c);
    }

    @Override
    public T getById(Object id) {
        @SuppressWarnings("unchecked") List<String> flds = getARHelper().getFields(type, Id.class);
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
        if (callback == null) {
            throw new IllegalArgumentException("Callback is null");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                List<T> ret = new ArrayList<>();
                T ent = get();
                ret.add(ent);
                callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, ret, ent);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(r);
    }

    @Override
    public T get() {
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
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
        Map<String, Object> fl = getFieldListForQuery();

        Map<String, Object> findMetaData = new HashMap<>();
        List<Map<String, Object>> srch;
        try {
            srch = morphium.getDriver().find(morphium.getConfig().getDatabase(), getCollectionName(), toQueryObject(), getSort(), fl, getSkip(), getLimit(), 1, getRP(), findMetaData);
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }

        if (srch.isEmpty()) {
            List<T> lst = new ArrayList<>(0);
            if (useCache) {
                morphium.getCache().addToCache(ck, type, lst);
            }
            return null;
        }

        Map<String, Object> ret;
        ret = srch.get(0);
        srv = (String) findMetaData.get("server");
        List<T> lst = new ArrayList<>(1);
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.GET);

        if (ret != null) {
            T unmarshall = morphium.getMapper().unmarshall(type, ret);
            if (unmarshall != null) {
                morphium.firePostLoadEvent(unmarshall);
                updateLastAccess(unmarshall);

                lst.add(unmarshall);
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
        if (callback == null) {
            throw new IllegalArgumentException("Callable is null?");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                List<Object> ret = idList();
                callback.onOperationSucceeded(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, null, null, ret);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, QueryImpl.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };

        getExecutor().submit(r);
    }

    @Override
    public <R> List<R> idList() {
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class);//type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();
        List<R> ret = new ArrayList<>();
        String ck = morphium.getCache().getCacheKey(this);
        ck += " idlist";
        morphium.inc(StatisticKeys.READS);
        if (useCache) {

            if (morphium.getCache().isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                //casts are not nice... any idea how to change that?
                //noinspection unchecked
                return (List<R>) morphium.getCache().getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();
        //        DBCollection collection = morphium.getDatabase().getCollection(getCollectionName());
        //        setReadPreferenceFor(collection);
        //                DBCursor query = collection.find(toQueryObject(), new HashMap<String, Object>("_id", 1)); //only get IDs
        Map<String, Object> findMetadata = new HashMap<>();

        List<Map<String, Object>> query;
        try {
            query = morphium.getDriver().find(morphium.getConfig().getDatabase(), getCollectionName(), toQueryObject(), null, Utils.getMap("_id", 1), skip, limit, 1, getRP(), findMetadata);
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }

        //noinspection unchecked
        ret.addAll(query.stream().map(o -> (R) o.get("_id")).collect(Collectors.toList()));
        srv = (String) findMetadata.get("server");
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.ID_LIST);
        if (useCache) {
            //noinspection unchecked
            morphium.getCache().addToCache(ck, (Class<? extends R>) type, ret);
        }
        return ret;
    }

    public Query<T> clone() throws CloneNotSupportedException {
        try {
            @SuppressWarnings("unchecked") QueryImpl<T> ret = (QueryImpl<T>) super.clone();
            if (andExpr != null) {
                ret.andExpr = new ArrayList<>();
                ret.andExpr.addAll(andExpr);
            }
            if (norQueries != null) {
                ret.norQueries = new ArrayList<>();
                ret.norQueries.addAll(norQueries);
            }
            if (sort != null) {
                ret.sort = new HashMap<>();
                ret.sort.putAll(sort);
            }
            if (orQueries != null) {
                ret.orQueries = new ArrayList<>();
                ret.orQueries.addAll(orQueries);
            }
            if (readPreferenceLevel != null) {
                ret.readPreferenceLevel = readPreferenceLevel;
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
    public Query<T> setCollectionName(String n) {
        collectionName = n;
        return this;
    }

    @Override
    public Query<T> text(String... text) {
        return text(null, null, text);
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
        f.setValue(Utils.getMap("$search", b.toString()));
        if (lang != null) {
            //noinspection unchecked
            ((Map<String, Object>) f.getValue()).put("$language", lang.toString());
        }
        addChild(f);
        if (metaScoreField != null) {
            additionalFields = Utils.getMap(metaScoreField, Utils.getMap("$meta", "textScore"));

        }

        return this;

    }

    @Override
    @Deprecated
    public List<T> textSearch(String... texts) {
        //noinspection deprecation
        return textSearch(TextSearchLanguages.mongo_default, texts);
    }

    @Override
    @Deprecated
    public List<T> textSearch(TextSearchLanguages lang, String... texts) {
        if (texts.length == 0) {
            return new ArrayList<>();
        }

        Map<String, Object> txt = new HashMap<>();
        txt.put("text", getCollectionName());
        StringBuilder b = new StringBuilder();
        for (String t : texts) {
            //            b.append("\"");
            b.append(t);
            b.append(" ");
            //            b.append("\" ");
        }
        txt.put("search", b.toString());
        txt.put("filter", toQueryObject());
        if (getLimit() > 0) {
            txt.put("limit", limit);
        }
        if (!lang.equals(TextSearchLanguages.mongo_default)) {
            txt.put("language", lang.name());
        }

        Map<String, Object> result;
        try {
            result = morphium.getDriver().runCommand(morphium.getConfig().getDatabase(), txt);
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }


        @SuppressWarnings("unchecked") List<Map<String, Object>> lst = (List<Map<String, Object>>) result.get("results");
        List<T> ret = new ArrayList<>();
        for (Object o : lst) {
            @SuppressWarnings("unchecked") Map<String, Object> obj = (Map<String, Object>) o;
            T unmarshall = morphium.getMapper().unmarshall(getType(), obj);
            if (unmarshall != null) {
                ret.add(unmarshall);
            }
        }
        return ret;
    }

    @Override
    public Query<T> setProjection(Enum... fl) {
        for (Enum f : fl) {
            addProjection(f);
        }
        return this;
    }

    @Override
    public Query<T> setProjection(String... fl) {
        fieldList = new HashMap<>();
        for (String f : fl) {
            addProjection(f);
        }
        return this;
    }

    @Override
    public Query<T> addProjection(Enum f, String projectOperator) {
        addProjection(f.name(), projectOperator);
        return this;
    }

    @Override
    public Query<T> addProjection(Enum f) {
        addProjection(f.name());
        return this;
    }

    @Override
    public Query<T> addProjection(String f) {
        if (fieldList == null) {
            fieldList = new HashMap<>();
        }
        String n = getARHelper().getFieldName(type, f);
        fieldList.put(n, 1);
        return this;
    }

    @Override
    public Query<T> addProjection(String f, String projectOperator) {
        if (fieldList == null) {
            fieldList = new HashMap<>();
        }
        String n = getARHelper().getFieldName(type, f);
        fieldList.put(n, projectOperator);
        return this;
    }

    @Override
    public Query<T> hideFieldInProjection(String f) {
        if (fieldList == null) {
            fieldList = new HashMap<>();

        }
        //        if (fieldList.size()==0){
        //            for (Field fld:getARHelper().getAllFields(type)){
        //                fieldList.put(getARHelper().getFieldName(type,fld.getName()),1); //enable all
        //            }
        //        }
        //        fieldList.remove(f);
        fieldList.put(getARHelper().getFieldName(type, f), 0);
        return this;
    }

    @Override
    public Query<T> hideFieldInProjection(Enum f) {
        return hideFieldInProjection(f.name());
    }

    @Override
    public String toString() {
        StringBuilder and = new StringBuilder();
        if (andExpr != null && !andExpr.isEmpty()) {
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
        if (orQueries != null && !orQueries.isEmpty()) {
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
        if (norQueries != null && !norQueries.isEmpty()) {
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
