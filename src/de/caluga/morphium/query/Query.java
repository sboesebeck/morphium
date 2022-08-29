package de.caluga.morphium.query;

import de.caluga.morphium.Collation;
import de.caluga.morphium.*;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Query<T> implements Cloneable {
    private static final Logger log = LoggerFactory.getLogger(Query.class);
    private String where;
    private Map<String, Object> rawQuery;
    private Class<? extends T> type;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;
    private ReadPreferenceLevel readPreferenceLevel;
    private boolean additionalDataPresent = false;
    private int limit = 0, skip = 0;
    private Map<String, Object> sort;
    private Morphium morphium;
    private ThreadPoolExecutor executor;
    private String collectionName;
    private String srv = null;

    private Map<String, Object> fieldList;

    private boolean autoValuesEnabled = true;

    private String tags;
    private AnnotationAndReflectionHelper arHelper;

    private String overrideDB;
    private Collation collation;
    private int batchSize = 0;
    private UtilsMap<String, UtilsMap<String, String>> additionalFields;

    public Query(Morphium m, Class<? extends T> type, ThreadPoolExecutor executor) {
        this(m);
        setType(type);
        this.executor = executor;
        if (m != null && m.getConfig().getDefaultTagSet() != null) {
            tags = m.getConfig().getDefaultTags();
        }
    }


    public Query(Morphium m) {
        setMorphium(m);
    }

    public int getBatchSize() {
        if (batchSize == 0) {
            return morphium.getDriver().getDefaultBatchSize();
        }
        return batchSize;
    }

    public Query<T> setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public Query<T> setCollation(Collation collation) {
        this.collation = collation;
        return this;
    }

    public String getDB() {
        if (overrideDB == null) {
            return morphium.getConfig().getDatabase();
        }
        return overrideDB;
    }

    public void overrideDB(String overrideDB) {
        this.overrideDB = overrideDB;
    }


    public String[] getTags() {
        if (tags == null) {
            return new String[0];
        }
        return tags.split(",");
    }


    public Query<T> addTag(String name, String value) {
        if (tags != null) {
            tags += ",";
        } else {
            tags = "";
        }
        tags += name + ":" + value;
        return this;
    }


    public Query<T> disableAutoValues() {
        autoValuesEnabled = false;
        return this;
    }


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


    public String getServer() {
        return srv;
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }


    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }


    public String getWhere() {
        return where;
    }


    public Morphium getMorphium() {
        return morphium;
    }


    public void setMorphium(Morphium m) {
        morphium = m;
        andExpr = new ArrayList<>();
        orQueries = new ArrayList<>();
        norQueries = new ArrayList<>();
        if (m == null) return;
        setARHelper(m.getARHelper());
    }

    public ReadPreferenceLevel getReadPreferenceLevel() {
        return readPreferenceLevel;
    }

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel) {
        this.readPreferenceLevel = readPreferenceLevel;
    }


    public Query<T> q() {
        Query<T> q = new Query<>(morphium, type, executor);
        q.setCollectionName(getCollectionName());
        return q;
    }

    /**
     * use rawQuery instead
     *
     * @param query
     * @return
     */
    @Deprecated
    public List<T> complexQuery(Map<String, Object> query) {
        return complexQuery(query, (String) null, 0, 0);
    }


    /**
     * use rawQuery() instead
     *
     * @param query
     * @param sort
     * @param skip
     * @param limit
     * @return
     */

    public List<T> complexQuery(Map<String, Object> query, String sort, int skip, int limit) {
        Map<String, Integer> srt = new LinkedHashMap<>();
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

    @SuppressWarnings("ConstantConditions")

    public T findOneAndDelete() {
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
                    morphium.delete(lst.get(0));
                    return lst.get(0);
                }

            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();

        Map<String, Object> ret = null;
        MongoConnection con = null;
        try {
            con = morphium.getDriver().getPrimaryConnection(null);
            FindAndModifyMongoCommand settings = new FindAndModifyMongoCommand(con)
                    .setQuery(toQueryObject())
                    .setRemove(true)
                    .setSort(new Doc(getSort()))
                    .setColl(getCollectionName())
                    .setDb(getDB());
            if (collation != null) settings.setCollation(Doc.of(collation.toQueryObject()));
            ret = settings.execute();
        } catch (MorphiumDriverException e) {
            e.printStackTrace();
        } finally {
            con.release();
        }

        if (ret == null) {
            List<T> lst = new ArrayList<>(0);
            if (useCache) {
                morphium.getCache().addToCache(ck, type, lst);
            }
            return null;
        }

        List<T> lst = new ArrayList<>(1);
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.GET);

        if (ret != null) {
            T unmarshall = morphium.getMapper().deserialize(type, ret);
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

    @SuppressWarnings("ConstantConditions")

    public T findOneAndUpdate(Map<String, Object> update) {
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
                    morphium.delete(lst.get(0));
                    return lst.get(0);
                }

            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();

        Map<String, Object> ret = null;
        MongoConnection con = null;
        try {
            con = morphium.getDriver().getPrimaryConnection(getMorphium().getWriteConcernForClass(getType()));

            FindAndModifyMongoCommand settings = new FindAndModifyMongoCommand(con)
                    .setDb(getDB())
                    .setColl(getCollectionName())
                    .setQuery(Doc.of(toQueryObject()))
                    .setUpdate(Doc.of(update));
            if (collation != null) settings.setCollation(Doc.of(collation.toQueryObject()));
            ret = settings.execute();
        } catch (MorphiumDriverException e) {
            e.printStackTrace();
        } finally {
            if (con != null)
                con.release();
        }

        if (ret == null) {
            List<T> lst = new ArrayList<>(0);
            if (useCache) {
                morphium.getCache().addToCache(ck, type, lst);
            }
            return null;
        }

        List<T> lst = new ArrayList<>(1);
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.GET);

        if (ret != null) {
            T unmarshall = morphium.getMapper().deserialize(type, ret);
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


    public T findOneAndUpdateEnums(Map<Enum, Object> update) {
        Map<String, Object> updates = new HashMap<>();
        for (Map.Entry<Enum, Object> e : update.entrySet()) {
            updates.put(e.getKey().name(), e.getValue());
        }
        return findOneAndUpdate(updates);
    }


    public AnnotationAndReflectionHelper getARHelper() {
        if (arHelper == null) {
            arHelper = morphium.getARHelper();
        }
        return arHelper;
    }


    public void setARHelper(AnnotationAndReflectionHelper ar) {
        arHelper = ar;
    }


    public long complexQueryCount(Map<String, Object> query) {
        long ret = 0;
        MongoConnection con = morphium.getDriver().getReadConnection(getRP());
        CountMongoCommand cmd = new CountMongoCommand(con)
                .setColl(collectionName)
                .setDb(getDB())
                .setQuery(Doc.of(query));
        Entity et = getARHelper().getAnnotationFromClass(getType(), Entity.class);
        if (et != null) {
            cmd.setReadConcern(Doc.of("level", et.readConcernLevel().name()));
        }
        try {
            ret = cmd.getCount();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        } finally {
            con.release();
        }
        return ret;
    }


    public Query<T> rawQuery(Map<String, Object> query) {
        if ((orQueries != null && !orQueries.isEmpty()) ||
                (norQueries != null && !norQueries.isEmpty()) ||
                (andExpr != null && !andExpr.isEmpty()) ||
                where != null) {
            throw new IllegalArgumentException("Cannot add raw query, when standard query already set!");
        }
        rawQuery = query;
        return this;
    }

    /**
     * use rawQuery to set query, and standard API
     *
     * @param query - query to be sent
     * @param sort
     * @param skip  - amount to skip
     * @param limit - maximium number of results
     * @return
     */
    @SuppressWarnings("DeprecatedIsStillUsed")

    @Deprecated
    public List<T> complexQuery(Map<String, Object> query, Map<String, Integer> sort, int skip, int limit) {
        Cache ca = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = ca != null && ca.readCache() && morphium.isReadCacheEnabledForThread();
        Map<String, Object> lst = getFieldListForQuery();
        String ck = morphium.getCache().getCacheKey(type, query, sort, lst, getCollectionName(), skip, limit);
        if (useCache && morphium.getCache().isCached(type, ck)) {
            return morphium.getCache().getFromCache(type, ck);
        }

        long start = System.currentTimeMillis();

        List<T> ret = new ArrayList<>();

        List<Map<String, Object>> obj = null;
        Map<String, Object> findMetaData = new HashMap<>();
        FindCommand settings = getFindCmd();
        try {
            settings.setFilter(query)
                    .setSkip(skip)
                    .setSort(new LinkedHashMap<>(sort))
                    .setLimit(limit);
            if (collation != null) settings.setCollation(getCollation().toQueryObject());
            obj = settings.execute();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        } finally {
            morphium.getDriver().releaseConnection(settings.getConnection());
        }
        for (Map<String, Object> in : obj) {
            T unmarshall = morphium.getMapper().deserialize(type, in);
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


    public Map<String, Object> getFieldListForQuery() {
        List<Field> fldlst = null;
        if (type != null) fldlst = getARHelper().getAllFields(type);
        Map<String, Object> lst = new HashMap<>();
        if (type != null) {
            Entity e = getARHelper().getAnnotationFromHierarchy(type, Entity.class);
            if (e.polymorph()) {
                //            lst.put("class_name", 1);
                return new HashMap<>();
            }
        }
        lst.put("_id", 1);
        if (fieldList != null) {
            lst.putAll(fieldList);
            boolean negative = true;
            for (Object v : fieldList.values()) {
                if (!v.equals(0)) {
                    negative = false;
                    break;
                }
            }

            boolean positive = true;
            for (Object v : fieldList.values()) {
                if (v.equals(0)) {
                    positive = false;
                    break;
                }
            }

            if (negative && positive) {
                throw new RuntimeException("Projection cannot add _and_ remove fields!");
            }
            if (negative) {
                lst.remove("_id");
            }
        } else {
            if (fldlst != null) {
                for (Field f : fldlst) {
                    if (f.isAnnotationPresent(AdditionalData.class)) {
                        //to enable additional data
                        lst = new HashMap<>();
                        break;
                    }
                    if (f.isAnnotationPresent(Aliases.class)) {
                        for (String n : f.getAnnotation(Aliases.class).value()) {
                            lst.put(n, 1);
                        }
                    }

                    String n = getARHelper().getMongoFieldName(type, f.getName());
                    // prevent Query failed with error code 16410 and error message 'FieldPath field names may not start with '$'.'
                    if (!n.startsWith("$jacoco")) {
                        lst.put(n, 1);
                    }
                }
            } else {
                lst = new HashMap<>();
            }
        }

        return lst;
    }


    public List distinct(String field) {
        MongoConnection con = morphium.getDriver().getReadConnection(getRP());
        try {
            var cmd = new DistinctMongoCommand(con)
                    .setDb(getDB())
                    .setColl(getCollectionName())
                    .setKey(field)
                    .setQuery(Doc.of(toQueryObject()));
            if (getCollation() != null) {
                cmd.setCollation(getCollation().toQueryObject());
            }
            return cmd.execute();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        } finally {
            con.release();
        }
    }


    public T complexQueryOne(Map<String, Object> query) {
        return complexQueryOne(query, null, 0);
    }


    public T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort, int skip) {
        List<T> ret = complexQuery(query, sort, skip, 1);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }
        return null;
    }


    public T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort) {
        return complexQueryOne(query, sort, 0);
    }


    public int getLimit() {
        return limit;
    }


    public int getSkip() {
        return skip;
    }


    public Map<String, Object> getSort() {
        return sort;
    }


    public Query<T> addChild(FilterExpression ex) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add child expression when raw query is defined!");
        }
        andExpr.add(ex);
        return this;
    }


    public Query<T> where(String wh) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add where when raw query is defined!");
        }
        where = wh;
        return this;
    }

    public MongoField<T> f(Enum f) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add field query when raw query is defined!");
        }
        return f(f.name());
    }


    public MongoField<T> f(String... f) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add field query when raw query is defined!");
        }
        StringBuilder b = new StringBuilder();
        for (String e : f) {
            b.append(e);
            b.append(".");
        }
        b.deleteCharAt(b.length() - 1);
        return f(b.toString());
    }


    public MongoField<T> f(Enum... f) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add field query when raw query is defined!");
        }
        StringBuilder b = new StringBuilder();
        for (Enum e : f) {
            b.append(e.name());
            b.append(".");
        }
        b.deleteCharAt(b.length() - 1);
        return f(b.toString());
    }

    @SuppressWarnings({"ConstantConditions", "CommentedOutCode"})
    public MongoField<T> f(String f) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add field query when raw query is defined!");
        }
        StringBuilder fieldPath = new StringBuilder();
        String cf;
        Class<?> clz = type;
        if (additionalDataPresent) {
            MongoField<T> fld = new MongoFieldImpl<>();
            fld.setFieldString(f);
            fld.setMapper(morphium.getMapper());
            fld.setQuery(this);
            log.debug("Not checking field name, additionalData is present");
            return fld;
        }
        if (f.contains(".") && !additionalDataPresent) {
            String[] fieldNames = f.split("\\.");
            for (String fieldName : fieldNames) {
                String fieldNameInstance = getARHelper().getMongoFieldName(clz, fieldName);
                Field field = getARHelper().getField(clz, fieldNameInstance);
                if (field == null) {
                    log.warn("Field " + fieldNameInstance + " not found!");
                } else {

                    //                if (field.isAnnotationPresent(Reference.class)) {
                    //                    //cannot join
                    //                    throw new IllegalArgumentException("cannot subquery references: " + fieldNameInstance + " of type " + clz.getName() + " has @Reference");
                    //                }
                    fieldPath.append(fieldNameInstance);
                    fieldPath.append('.');
                    clz = field.getType();
                    if (List.class.isAssignableFrom(clz) || Collection.class.isAssignableFrom(clz) || Array.class.isAssignableFrom(clz) || Set.class.isAssignableFrom(clz) || Map.class.isAssignableFrom(clz)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Cannot check fields in generic lists or maps");
                        }
                        clz = Object.class;
                    }
                    if (clz.equals(Object.class)) {
                        break;
                    }
                }
            }
            if (clz.equals(Object.class)) {
                cf = f;
            } else {
                cf = fieldPath.substring(0, fieldPath.length() - 1);
            }
        } else {
            cf = getARHelper().getMongoFieldName(clz, f);

        }
        MongoField<T> fld = new MongoFieldImpl<>();
        fld.setFieldString(cf);
        fld.setMapper(morphium.getMapper());
        fld.setQuery(this);
        return fld;
    }

    @SafeVarargs

    public final Query<T> or(Query<T>... qs) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add or queries when raw query is defined!");
        }
        orQueries.addAll(Arrays.asList(qs));
        return this;
    }


    public Query<T> or(List<Query<T>> qs) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add or queries when raw query is defined!");
        }
        orQueries.addAll(qs);
        return this;
    }

    @SuppressWarnings("unused")
    private Query<T> getClone() {
        return clone();
    }

    @SafeVarargs

    public final Query<T> nor(Query<T>... qs) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add nor queries when raw query is defined!");
        }
        norQueries.addAll(Arrays.asList(qs));
        return this;
    }


    public Query<T> limit(int i) {
        limit = i;
        return this;
    }


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

    public Query<T> sort(Map<String, Integer> n) {
        sort = new LinkedHashMap<>();
        for (var e : n.entrySet()) {
            sort.put(e.getKey(), e.getValue());
        }
        return this;
    }


    public Query<T> sortEnum(Map<Enum, Integer> n) {
        sort = new HashMap<>();
        for (Map.Entry<Enum, Integer> e : n.entrySet()) {
            sort.put(morphium.getARHelper().getMongoFieldName(getType(), e.getKey().name()), e.getValue());
        }

        return this;
    }


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
                fld = getARHelper().getMongoFieldName(type, fld);
            }
            m.put(fld, val);
        }
        return sort(m);
    }


    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Integer> m = new LinkedHashMap<>();
        for (Enum i : naturalOrder) {
            String fld = getARHelper().getMongoFieldName(type, i.name());
            m.put(fld, 1);
        }
        return sort(m);
    }


    public void countAll(final AsyncOperationCallback<T> c) {
        if (c == null) {
            throw new IllegalArgumentException("Not really useful to read from db and not use the result");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                long ret = countAll();
                c.onOperationSucceeded(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, null, null, ret);
            } catch (Exception e) {
                c.onOperationError(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };

        getExecutor().submit(r);

    }


    public long countAll() {
        morphium.inc(StatisticKeys.READS);
        long start = System.currentTimeMillis();
        long ret;
        if (where != null) {
            log.warn("efficient counting with $where is not possible... need to iterate!");
            int lim = limit;
            int sk = skip;
            skip = 0;
            limit = 0;
            Map<String, Object> fld = fieldList;

            fieldList = null;
            addProjection("_id"); //only read ids
            int count = 0;
            for (T elem : asIterable()) {
                count++;
            }
            limit = lim;
            skip = sk;
            fieldList = fld;
            return count;

        }
        MongoConnection con = getMorphium().getDriver().getReadConnection(getRP());
        try {
            if (andExpr.isEmpty() && orQueries.isEmpty() && norQueries.isEmpty() && rawQuery == null) {
                try {
                    CountMongoCommand settings = new CountMongoCommand(con).setDb(getDB())
                            .setColl(getCollectionName())
                            //.setQuery(Doc.of(this.toQueryObject()))
                            ;
                    if (getCollation() != null)
                        settings.setCollation(getCollation().toQueryObject());
                    ret = settings.getCount();
//                            .setReadConcern(getRP().);
                } catch (MorphiumDriverException e) {
                    log.error("Error counting", e);
                    ret = 0;
                }
            } else {
                try {
                    var cmd = new CountMongoCommand(con)
                            .setDb(getDB())
                            .setColl(getCollectionName())
                            .setQuery(Doc.of(toQueryObject()));
                    if (getCollation() != null)
                        cmd.setCollation(getCollation().toQueryObject());
                    ret = cmd.getCount();
                } catch (MorphiumDriverException e) {
                    // TODO: Implement Handling
                    throw new RuntimeException(e);
                }
            }
        } finally {
            con.release();
        }
        morphium.fireProfilingReadEvent(Query.this, System.currentTimeMillis() - start, ReadAccessType.COUNT);
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


    public Map<String, Object> toQueryObject() {
        if (this.rawQuery != null) {
            return this.rawQuery;
        }
        Map<String, Object> o = new LinkedHashMap<>();
        List<Map<String, Object>> lst = new ArrayList<>();
        boolean onlyAnd = orQueries.isEmpty() && norQueries.isEmpty() && where == null;
        if (where != null) {
            o.put("$where", where);
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
                        add(UtilsMap.of("$or", lst));
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
                        add(UtilsMap.of("$nor", lst));
            } else {
                o.put("$nor", lst);
            }
        }


        return o;
    }


    public Query<T> expr(Expr exp) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add filter expression when raw query is defined!");
        }
        FilterExpression fe = new FilterExpression();
        fe.setField("$expr");

        fe.setValue(exp.toQueryObject());
        andExpr.add(fe);
        return this;
    }


    public Query<T> matchesJsonSchema(Map<String, Object> schemaDef) {
        if (rawQuery != null) {
            throw new IllegalArgumentException("Cannot add jason schema match when raw query is defined!");
        }
        FilterExpression fe = new FilterExpression();
        fe.setField("$jsonSchema");
        fe.setValue(schemaDef);
        andExpr.add(fe);

        return this;
    }

    @SuppressWarnings("unchecked")

    public Query<T> matchesJsonSchema(String schemaDef) throws ParseException {
        JSONParser jsonParser = new JSONParser();

        @SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) jsonParser.parse(schemaDef, new ContainerFactory() {

                    public Map createObjectContainer() {
                        return new HashMap<>();
                    }


                    public List creatArrayContainer() {
                        return new ArrayList();
                    }
                }
        );

        return matchesJsonSchema(map);
    }


    public Class<? extends T> getType() {
        return type;
    }


    public void setType(Class<? extends T> type) {
        this.type = type;
        if (morphium == null) return;
        DefaultReadPreference pr = getARHelper().getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            setReadPreferenceLevel(pr.value());
        }
        @SuppressWarnings("unchecked") List<String> fields = getARHelper().getFields(type, AdditionalData.class);
        additionalDataPresent = fields != null && !fields.isEmpty();
    }


    public void asList(final AsyncOperationCallback<T> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("callback is null");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                List<T> lst = asList();
                callback.onOperationSucceeded(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, lst, null);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(r);
    }

    @SuppressWarnings("unchecked")

    public List<Map<String, Object>> asMapList() {
        morphium.inc(StatisticKeys.READS);
        if (type != null) {
            Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
            boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();
            Class type = Map.class;
            String ck = morphium.getCache().getCacheKey(this);
            if (useCache) {
                if (morphium.getCache().isCached(type, ck)) {
                    morphium.inc(StatisticKeys.CHITS);
                    //noinspection unchecked
                    return morphium.getCache().getFromCache(type, ck);
                }
                morphium.inc(StatisticKeys.CMISS);
            } else {
                morphium.inc(StatisticKeys.NO_CACHED_READS);

            }
            long start = System.currentTimeMillis();

            Map<String, Object> lst = getFieldListForQuery();


            List<Map<String, Object>> ret = new ArrayList<>();
            try {

                ret = getFindCmd().execute();

            } catch (Exception e) {
                throw new RuntimeException(e);

            }
            morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);

            if (useCache) {
                //noinspection unchecked
                morphium.getCache().addToCache(ck, type, ret);
            }
            morphium.firePostLoad(ret);
            List<Map<String, Object>> result = new ArrayList<>();
            for (var o : ret) {
                result.add(new HashMap<>(o));
            }
            return result;
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);

            long start = System.currentTimeMillis();

            Map<String, Object> lst = getFieldListForQuery();


            List<Map<String, Object>> ret = new ArrayList<>();
            try {

                FindCommand settings = getFindCmd();
                ret = settings.execute();
                srv = (String) settings.getMetaData().get("server");
            } catch (Exception e) {
                throw new RuntimeException(e);

            }
            morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.AS_LIST);
            return new ArrayList<>(ret);
        }
    }


    public QueryIterator<Map<String, Object>> asMapIterable() {
        QueryIterator<Map<String, Object>> it = new QueryIterator<>();
        it.setQuery((Query<Map<String, Object>>) this);
        return it;
    }


    public List<T> asList() {
        if (type == null) {
            return (List<T>) asMapList();
        }
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
            FindCommand cmd = getFindCmd();
            Map<String, Object> queryObject = toQueryObject();
            if (queryObject != null) cmd.setFilter(Doc.of(queryObject));
            if (collation != null) cmd.setCollation(Doc.of(collation.toQueryObject()));
            if (sort != null) cmd.setSort(new Doc(sort));
            var query = cmd.execute();
            srv = (String) cmd.getMetaData().get("server");


            for (Map<String, Object> o : query) {
                T unmarshall = morphium.getMapper().deserialize(type, o);
                if (unmarshall != null) {
                    ret.add(unmarshall);
                    updateLastAccess(unmarshall);
                    morphium.firePostLoadEvent(unmarshall);
                }
            }
            cmd.getConnection().release();

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


    public QueryIterator<T> asIterable() {
        QueryIterator<T> it = new QueryIterator<>();
        it.setQuery(this);
        return it;
    }


    public QueryIterator<T> asIterable(int windowSize) {
        QueryIterator<T> it = new QueryIterator<>();
        it.setWindowSize(windowSize);
        it.setQuery(this);
        return it;
    }


    public QueryIterator<T> asIterable(QueryIterator<T> ret) {
        try {
            ret.setQuery(this);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MorphiumCursor getCursor() throws MorphiumDriverException {
        return getFindCmd().executeIterable(getBatchSize());
    }


    public QueryIterator<T> asIterable(int windowSize, Class<? extends QueryIterator<T>> it) {
        try {
            QueryIterator<T> ret = it.getDeclaredConstructor().newInstance();
//            ret.setWindowSize(windowSize);
            return asIterable(ret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @SuppressWarnings("CommentedOutCode")
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
                    MongoConnection con = null;
                    try {
                        con = getMorphium().getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(getType()));
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
                        Object id = getARHelper().getId(unmarshall);
                        //Cannot use store, as this would trigger an update of last changed...

                        UpdateMongoCommand settings = new UpdateMongoCommand(con)
                                .setDb(getDB())
                                .setColl(getCollectionName())
                                .setUpdates(Arrays.asList(Doc.of("q", Doc.of("_id", id), "u", Doc.of("$set", Doc.of(ctf, currentTime)), "multi", false, "collation", collation != null ? Doc.of(collation.toQueryObject()) : null)));
                        settings.execute();
                    } catch (Exception e) {
                        log.error("Could not set modification time");
                        throw new RuntimeException(e);
                    } finally {
                        if (con != null)
                            con.release();
                    }
                }
            }

            //Storing access timestamps
            //            List<T> l=new ArrayList<T>();
            //            l.add(deserialize);
            //            morphium.getWriterForClass(deserialize.getClass()).store(l,null);

            //            morphium.store(deserialize);
        }
    }


    @Deprecated
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
                callback.onOperationSucceeded(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, result, res);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(c);
    }


    @Deprecated
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
                callback.onOperationSucceeded(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, ret, ent);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };
        getExecutor().submit(r);
    }


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
        List<Map<String, Object>> srch = null;
        int lim = getLimit();
        limit(1);
        try {
            FindCommand settings = getFindCmd();
            srch = settings.execute();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
        limit(lim);
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
            T unmarshall = morphium.getMapper().deserialize(type, ret);
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


    public void idList(final AsyncOperationCallback<T> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Callable is null?");
        }
        Runnable r = () -> {
            long start = System.currentTimeMillis();
            try {
                List<Object> ret = idList();
                callback.onOperationSucceeded(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, null, null, ret);
            } catch (Exception e) {
                callback.onOperationError(AsyncOperationType.READ, Query.this, System.currentTimeMillis() - start, e.getMessage(), e, null);
            }
        };

        getExecutor().submit(r);
    }

    @SuppressWarnings("CommentedOutCode")

    public <R> List<R> idList() {
        Cache c = getARHelper().getAnnotationFromHierarchy(type, Cache.class);//type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache() && morphium.isReadCacheEnabledForThread();
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


        List<Map<String, Object>> query;
        try {

            FindCommand settings = getFindCmd();
            settings.setProjection(Doc.of("_id", 1));
            query = settings.execute();
            srv = (String) settings.getMetaData().get("server");
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }

        //noinspection unchecked
        List<R> ret = query.stream().map(o -> (R) o.get("_id")).collect(Collectors.toList());
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.ID_LIST);
        if (useCache) {
            //noinspection unchecked
            morphium.getCache().addToCache(ck, (Class<? extends R>) type, ret);
        }
        return ret;
    }

    public Query<T> clone() {
        try {
            @SuppressWarnings("unchecked") Query<T> ret = (Query<T>) super.clone();
            if (andExpr != null) {
                ret.andExpr = new ArrayList<>();
                ret.andExpr.addAll(andExpr);
            }
            if (norQueries != null) {
                ret.norQueries = new ArrayList<>();
                ret.norQueries.addAll(norQueries);
            }
            if (sort != null) {
                ret.sort = new LinkedHashMap<>();
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


    public void delete() {
        morphium.delete(this);
    }


    public void set(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.set(this, field, value, upsert, multiple, cb);
    }


    public void set(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.set(this, field, value, upsert, multiple, cb);
    }


    public void setEnum(Map<Enum, Object> map, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<Enum, Object> e : map.entrySet()) {
            m.put(e.getKey().name(), e.getValue());
        }
        set(m, upsert, multiple, cb);
    }


    public void set(Map<String, Object> map, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.set(this, map, upsert, multiple, cb);
    }


    public void set(String field, Object value, AsyncOperationCallback<T> cb) {
        morphium.set(this, field, value, cb);
    }


    public void set(Enum field, Object value, AsyncOperationCallback<T> cb) {
        morphium.set(this, field, value, cb);
    }


    public void setEnum(Map<Enum, Object> map, AsyncOperationCallback<T> cb) {
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<Enum, Object> e : map.entrySet()) {
            m.put(e.getKey().name(), e.getValue());
        }
        set(m, false, false, cb);
    }


    public void set(Map<String, Object> map, AsyncOperationCallback<T> cb) {
        morphium.set(this, map, false, false, cb);
    }


    public void set(String field, Object value, boolean upsert, boolean multiple) {
        morphium.set(this, field, value, upsert, multiple);
    }


    public void set(Enum field, Object value, boolean upsert, boolean multiple) {
        morphium.set(this, field, value, upsert, multiple);
    }


    public void setEnum(Map<Enum, Object> map, boolean upsert, boolean multiple) {
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<Enum, Object> e : map.entrySet()) {
            m.put(e.getKey().name(), e.getValue());
        }
        set(m, upsert, multiple, null);
    }


    public void set(Map<String, Object> map, boolean upsert, boolean multiple) {
        morphium.set(this, map, upsert, multiple);
    }


    public void set(String field, Object value) {
        morphium.set(this, field, value);
    }


    public void set(Enum field, Object value) {
        morphium.set(this, field, value);
    }


    public void setEnum(Map<Enum, Object> map) {
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<Enum, Object> e : map.entrySet()) {
            m.put(e.getKey().name(), e.getValue());
        }
        set(m, false, false, null);
    }


    public void set(Map<String, Object> map) {
        morphium.set(this, map, false, false);
    }


    public void push(String field, Object value) {
        morphium.push(this, field, value);
    }


    public void push(Enum field, Object value) {
        morphium.push(this, field, value);
    }


    public void push(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.push(this, field, value, upsert, multiple, cb);
    }


    public void push(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.push(this, field.name(), value, upsert, multiple, cb);
    }


    public void pushAll(String field, List value) {
        morphium.pushAll(this, field, value, false, false);
    }


    public void pushAll(Enum field, List value) {
        morphium.pushAll(this, field.name(), value, false, false);
    }


    public void pushAll(String field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pushAll(this, field, value, upsert, multiple, cb);
    }


    public void pushAll(Enum field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pushAll(this, field.name(), value, upsert, multiple, cb);
    }


    public void pullAll(String field, List value) {
        //noinspection unchecked
        morphium.pullAll(this, field, value, false, false);
    }


    public void pullAll(Enum field, List value) {
        //noinspection unchecked
        morphium.pullAll(this, field.name(), value, false, false);
    }


    public void pullAll(String field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pullAll(this, field, value, upsert, multiple, cb);
    }


    public void pullAll(Enum field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pullAll(this, field.name(), value, upsert, multiple, cb);
    }


    public void pull(String field, Object value) {
        morphium.pull(this, field, value);
    }


    public void pull(Enum field, Object value) {
        morphium.pull(this, field, value);
    }


    public void pull(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pull(this, field, value, upsert, multiple, cb);
    }


    public void pull(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pull(this, field.name(), value, upsert, multiple, cb);
    }


    public void pull(Enum field, Expr value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.pull(this, field.name(), value.toQueryObject(), upsert, multiple, cb);
    }


    public void pull(Enum field, Expr value, boolean upsert, boolean multiple) {
        pull(field, value, upsert, multiple, null);
    }


    public void pull(Enum field, Expr value) {
        pull(field, value, false, false, null);
    }


    public void inc(String field, Integer value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.inc(this, field, value, upsert, multiple, cb);
    }


    public void inc(String field, Integer value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(Enum field, Integer value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(String field, Integer value) {
        morphium.inc(this, field, value);
    }


    public void inc(Enum field, Integer value) {
        morphium.inc(this, field, value);
    }


    public void inc(String field, Double value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.inc(this, field, value, upsert, multiple, cb);
    }


    public void inc(String field, Double value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(Enum field, Double value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(String field, Double value) {
        morphium.inc(this, field, value);
    }


    public void inc(Enum field, Double value) {
        morphium.inc(this, field, value);
    }


    public void inc(Enum field, Number value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        inc(field.name(), value, upsert, multiple, cb);
    }


    public void inc(Enum field, Integer value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        inc(field.name(), value, upsert, multiple, cb);
    }


    public void inc(Enum field, Double value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        inc(field.name(), value, upsert, multiple, cb);
    }


    public void inc(Enum field, Long value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        inc(field.name(), value, upsert, multiple, cb);
    }


    public void inc(String field, Long value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.inc(this, field, value, upsert, multiple, cb);
    }


    public void inc(String field, Long value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(Enum field, Long value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(String field, Long value) {
        morphium.inc(this, field, value);
    }


    public void inc(Enum field, Long value) {
        morphium.inc(this, field, value);
    }


    public void inc(String field, Number value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.inc(this, field, value, upsert, multiple, cb);
    }


    public void inc(String field, Number value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(Enum field, Number value, boolean upsert, boolean multiple) {
        morphium.inc(this, field, value, upsert, multiple);
    }


    public void inc(String field, Number value) {
        morphium.inc(this, field, value);
    }


    public void inc(Enum field, Number value) {
        morphium.inc(this, field, value);
    }


    public void dec(String field, Integer value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(Enum field, Integer value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(String field, Integer value) {
        morphium.dec(this, field, value);
    }


    public void dec(Enum field, Integer value) {
        morphium.dec(this, field, value);
    }


    public void dec(Enum field, Integer value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(String field, Integer value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(Enum field, Double value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(String field, Double value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(Enum field, Long value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {

    }


    public void dec(String field, Long value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(Enum field, Number value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(String field, Number value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb) {
        morphium.dec(this, field, value, upsert, multiple, cb);
    }


    public void dec(String field, Double value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(Enum field, Double value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(String field, Double value) {
        morphium.dec(this, field, value);
    }


    public void dec(Enum field, Double value) {
        morphium.dec(this, field, value);
    }


    public void dec(String field, Long value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(Enum field, Long value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(String field, Long value) {
        morphium.dec(this, field, value);
    }


    public void dec(Enum field, Long value) {
        morphium.dec(this, field, value);
    }


    public void dec(String field, Number value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(Enum field, Number value, boolean upsert, boolean multiple) {
        morphium.dec(this, field, value, upsert, multiple);
    }


    public void dec(String field, Number value) {
        morphium.dec(this, field, value);
    }


    public void dec(Enum field, Number value) {
        morphium.dec(this, field, value);
    }


    public int getNumberOfPendingRequests() {
        return getExecutor().getActiveCount();
    }


    public String getCollectionName() {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }
        return collectionName;
    }


    public Query<T> setCollectionName(String n) {
        collectionName = n;
        return this;
    }


    public Query<T> text(String... text) {
        return text(null, null, text);
    }


    public Query<T> text(TextSearchLanguages lang, String... text) {
        return text(null, lang, text);
    }


    public Query<T> text(TextSearchLanguages lang, boolean caseSensitive, boolean diacriticSensitive, String... text) {
        return text(null, lang, caseSensitive, diacriticSensitive, text);
    }


    public Query<T> text(String metaScoreField, TextSearchLanguages lang, boolean caseSensitive, boolean diacriticSensitive, String... text) {
        FilterExpression f = new FilterExpression();
        f.setField("$text");
        StringBuilder b = new StringBuilder();
        for (String t : text) {
            b.append(t);
            b.append(" ");
        }
        Map<String, Object> srch = UtilsMap.of("$search", b.toString());
        srch.put("$caseSensitive", caseSensitive);
        srch.put("$diacriticSensitive", diacriticSensitive);
        f.setValue(srch);
        if (lang != null) {
            //noinspection unchecked
            ((Map<String, Object>) f.getValue()).put("$language", lang.toString());
        }
        addChild(f);
        if (metaScoreField != null) {
            additionalFields = UtilsMap.of(metaScoreField, UtilsMap.of("$meta", "textScore"));

        }

        return this;
    }


    public Query<T> text(String metaScoreField, TextSearchLanguages lang, String... text) {
        return text(metaScoreField, lang, true, true, text);
    }


    @Deprecated
    public List<T> textSearch(String... texts) {
        //noinspection deprecation
        return textSearch(TextSearchLanguages.mongo_default, texts);
    }

    @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")

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

        Map<String, Object> result = null;
        try {
            GenericCommand cmd = new GenericCommand(morphium.getDriver().getPrimaryConnection(null));
            cmd.setDb(getDB());
            cmd.setCmdData(Doc.of(txt));
            result = cmd.getConnection().readSingleAnswer(cmd.executeAsync());
            cmd.getConnection().release();
        } catch (MorphiumDriverException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }


        @SuppressWarnings("unchecked") List<Map<String, Object>> lst = (List<Map<String, Object>>) result.get("results");
        List<T> ret = new ArrayList<>();
        for (Object o : lst) {
            @SuppressWarnings("unchecked") Map<String, Object> obj = (Map<String, Object>) o;
            T unmarshall = morphium.getMapper().deserialize(getType(), obj);
            if (unmarshall != null) {
                ret.add(unmarshall);
            }
        }
        return ret;
    }


    public Query<T> setProjection(Enum... fl) {
        for (Enum f : fl) {
            addProjection(f);
        }
        return this;
    }


    public Query<T> setProjection(String... fl) {
        fieldList = new LinkedHashMap<>();
        for (String f : fl) {
            addProjection(f);
        }
        return this;
    }


    public Query<T> addProjection(Enum f, String projectOperator) {
        addProjection(f.name(), projectOperator);
        return this;
    }


    public Query<T> addProjection(Enum f) {
        addProjection(f.name());
        return this;
    }


    public Query<T> addProjection(String f) {
        if (fieldList == null) {
            fieldList = new LinkedHashMap<>();
        }
        int v = 1;

        if (f.startsWith("-")) {
            f = f.substring(1);
            v = 0;
        }
        String n = getARHelper().getMongoFieldName(type, f);
        fieldList.put(n, v);
        return this;
    }


    public Query<T> addProjection(String f, String projectOperator) {
        if (fieldList == null) {
            fieldList = new LinkedHashMap<>();
        }
        String n = getARHelper().getMongoFieldName(type, f);
        fieldList.put(n, projectOperator);
        return this;
    }

    @SuppressWarnings("CommentedOutCode")

    public Query<T> hideFieldInProjection(String f) {
        if (fieldList == null || fieldList.isEmpty()) {
            fieldList = new LinkedHashMap<>();

        }
        fieldList.put(getARHelper().getMongoFieldName(type, f), 0);
        return this;
    }

    /**
     * do a tail query
     *
     * @param batchSize - determins how much data is read in one step
     * @param maxWait   - how long to wait _at most_ for data, throws Exception otherwise
     * @param cb        - the callback being called for _every single document_ - the entity field is filled, lists will be null
     */

    public void tail(int batchSize, int maxWait, AsyncOperationCallback<T> cb) {
        var con = morphium.getDriver().getReadConnection(morphium.getReadPreferenceForClass(type));
        boolean running = true;
        if (maxWait == 0) maxWait = Integer.MAX_VALUE;
        try {
            FindCommand cmd = new FindCommand(con)
                    .setTailable(true)
                    .setFilter(toQueryObject())
                    .setSort(getSort())
                    .setLimit(getLimit())
                    .setBatchSize(batchSize)
                    .setMaxTimeMS(maxWait)
                    .setDb(morphium.getDatabase())
                    .setColl(getCollectionName());
            if (collation != null) cmd.setCollation(collation.toQueryObject());
            long start = System.currentTimeMillis();

            var msgId = cmd.executeAsync();
            long cursorId = 0;
            while (running) {
                var answer = con.getReplyFor(msgId, maxWait);
                List<Map<String, Object>> batch = null;
                Map<String, Object> cursor = (Map<String, Object>) answer.getFirstDoc().get("cursor");
                if (cursor == null) {
                    log.warn("No cursor in result");
                    break;
                }
                if (cursor.containsKey("firstBatch")) {
                    batch = (List<Map<String, Object>>) cursor.get("firstBatch");
                } else if (cursor.containsKey("nextBatch")) {
                    batch = (List<Map<String, Object>>) cursor.get("nextBatch");
                } else {
                    batch = new ArrayList<>();
                }

                for (Map<String, Object> doc : batch) {
                    try {
                        cb.onOperationSucceeded(AsyncOperationType.READ, this, System.currentTimeMillis() - start, null, morphium.getMapper().deserialize(type, doc));
                    } catch (MorphiumAccessVetoException ex) {
                        running = false;
                        break;
                    }
                }
                cursorId = ((Number) cursor.get("id")).longValue();
                GetMoreMongoCommand more = new GetMoreMongoCommand(con).setBatchSize(batchSize)
                        .setColl(getCollectionName())
                        .setDb(cmd.getDb())
                        .setCursorId(cursorId);
                msgId = more.executeAsync();
            }
            if (cursorId != 0) {
                //killing cursors
                KillCursorsCommand kill = new KillCursorsCommand(con).setCursorIds(cursorId).setColl(cmd.getColl()).setDb(cmd.getDb());
                kill.execute();
            }
            log.info("Tail ended!");
        } catch (Exception e) {
            throw new RuntimeException("Error running command", e);
        } finally {
            con.release();
        }
    }


    public Query<T> hideFieldInProjection(Enum f) {
        return hideFieldInProjection(f.name());
    }


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
                ", andExpr=" + and +
                ", orQueries=" + ors +
                ", norQueries=" + nors +
                ", sort=" + sort +
                ", readPreferenceLevel=" + readPreferenceLevel +
                ", additionalDataPresent=" + additionalDataPresent +
                ", where='" + where + '\'' +
                '}';
        if (fieldList != null) {
            ret += " Fields " + fieldList;

        }
        return ret;
    }

    public FindCommand getFindCmd() {
        MongoConnection con = getMorphium().getDriver().getReadConnection(getRP());
        FindCommand settings = new FindCommand(con)
                .setDb(getMorphium().getConfig().getDatabase())
                .setColl(getCollectionName())
                .setFilter(toQueryObject())
                .setSort(getSort())
                .setProjection(getFieldListForQuery())
                .setSkip(getSkip())
                .setLimit(getLimit())
                .setReadPreference(getMorphium().getReadPreferenceForClass(getType()));
        settings.setBatchSize(getBatchSize());
        return settings;
    }


    public enum TextSearchLanguages {
        danish,
        dutch,
        english,
        finnish,
        french,
        german,
        hungarian,
        italian,
        norwegian,
        portuguese,
        romanian,
        russian,
        spanish,
        swedish,
        turkish,
        mongo_default,
        none,

    }
}
