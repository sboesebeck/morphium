package de.caluga.morphium.query;

import com.mongodb.*;
import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ReadAccessType;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.secure.Permission;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 22:14
 * <p/>
 */
public class QueryImpl<T> implements Query<T>, Cloneable {
    private String where;
    private Class<T> type;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;
    private ReadPreferenceLevel readPreferenceLevel;
    private ReadPreference readPreference;

    private int limit = 0, skip = 0;
    private Map<String, Integer> order;

    private Morphium morphium;
    private static Logger log = Logger.getLogger(Query.class);

    public QueryImpl() {

    }

    public QueryImpl(Morphium m, Class<T> type) {
        this(m);
        setType(type);
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

    public QueryImpl(Morphium m) {
        setMorphium(m);
    }

    public ReadPreferenceLevel getReadPreferenceLevel() {
        return readPreferenceLevel;
    }

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel) {
        if (morphium.getConfig().isTimeoutBugWorkAroundEnabled() && !readPreferenceLevel.getPref().equals(ReadPreference.primary())) {
//            Logger.getLogger(QueryImpl.class).warn("Not setting read Preference level in bug-workaround-mode! Using default");
            readPreferenceLevel = ReadPreferenceLevel.PRIMARY;
            readPreference = ReadPreference.primary();
            return;
        }
        this.readPreferenceLevel = readPreferenceLevel;
        readPreference = readPreferenceLevel.getPref();
    }

    @Override
    public void setType(Class<T> type) {
        this.type = type;
        DefaultReadPreference pr = morphium.getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            setReadPreferenceLevel(pr.value());

        }
    }

    @Override
    public Query<T> q() {
        Query<T> q = new QueryImpl<T>(morphium, type);
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
        if (morphium.accessDenied(type, Permission.READ)) {
            throw new RuntimeException("Access denied!");
        }

        String ck = morphium.getCacheKey(query, sort, skip, limit);
        if (morphium.isCached(type, ck)) {
            return morphium.getFromCache(type, ck);
        }
        long start = System.currentTimeMillis();
        DBCollection c = morphium.getDatabase().getCollection(morphium.getConfig().getMapper().getCollectionName(type));
        setReadPreferenceFor(c);
        List<Field> fldlst = morphium.getMapper().getAllFields(type);
        BasicDBObject lst = new BasicDBObject();
        lst.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                lst = new BasicDBObject();
                break;
            }
            String n = morphium.getMapper().getFieldName(type, f.getName());
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
            ret.add(morphium.getConfig().getMapper().unmarshall(type, cursor.next()));
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
        if (ret != null && ret.isEmpty()) {
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
    public Map<String, Integer> getOrder() {
        return order;
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
        }
        Field field = morphium.getMapper().getField(type, cf);
        if (field == null) {
            throw new IllegalArgumentException("Unknown Field " + f);
        }

        if (field.isAnnotationPresent(Id.class)) {
            f = "_id";
        } else {
            f = morphium.getMapper().getFieldName(type, f); //handling of aliases
        }
        MongoField<T> fld = morphium.createMongoField(); //new MongoFieldImpl<T>();
        fld.setFieldString(f);
        fld.setMapper(morphium.getMapper());
        fld.setQuery(this);
        return fld;
    }

    @Override
    public void or(Query<T>... qs) {
        orQueries.addAll(Arrays.asList(qs));
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
    public void nor(Query<T>... qs) {
        norQueries.addAll(Arrays.asList(qs));
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
        order = n;
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
                fld = morphium.getMapper().getFieldName(type, fld);
            }
            m.put(fld, val);
        }
        return sort(m);
    }

    @Override
    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Integer> m = new LinkedHashMap<String, java.lang.Integer>();
        for (Enum i : naturalOrder) {
            String fld = morphium.getMapper().getFieldName(type, i.name());
            m.put(fld, 1);
        }
        return sort(m);
    }

    @Override
    public long countAll() {
        if (morphium.accessDenied(type, Permission.READ)) {
            throw new RuntimeException("Access denied!");
        }
        morphium.inc(StatisticKeys.READS);

        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(type));
        setReadPreferenceFor(collection);
        long ret = collection.count(toQueryObject());
        morphium.fireProfilingReadEvent(this, System.currentTimeMillis() - start, ReadAccessType.COUNT);
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
    public Class<T> getType() {
        return type;
    }

    @Override
    public List<T> asList() {
        if (morphium.accessDenied(type, Permission.READ)) {
            throw new RuntimeException("Access denied!");
        }
        morphium.inc(StatisticKeys.READS);
        Cache c = morphium.getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache();

        String ck = morphium.getCacheKey(this);
        if (useCache) {
            if (morphium.isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                return morphium.getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);

        }
        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(type));
        setReadPreferenceFor(collection);

        List<Field> fldlst = morphium.getMapper().getAllFields(type);
        BasicDBObject lst = new BasicDBObject();
        lst.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                lst = new BasicDBObject();
                break;
            }
            String n = morphium.getMapper().getFieldName(type, f.getName());
            lst.put(n, 1);
        }

        DBCursor query = collection.find(toQueryObject(), lst);
        if (skip > 0) {
            query.skip(skip);
        }
        if (limit > 0) {
            query.limit(limit);
        }
        if (order != null) {
            BasicDBObject srt = new BasicDBObject();
            for (String k : order.keySet()) {
                srt.append(k, order.get(k));
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
            morphium.addToCache(ck, type, ret);
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
        if (morphium.isAnnotationPresentInHierarchy(type, LastAccess.class)) {
            List<String> lst = morphium.getMapper().getFields(type, LastAccess.class);
            for (String ctf : lst) {
                Field f = morphium.getMapper().getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(unmarshall, System.currentTimeMillis());
                    } catch (IllegalAccessException e) {
                        System.out.println("Could not set modification time");

                    }
                }
            }
            lst = morphium.getMapper().getFields(type, LastAccessBy.class);
            for (String ctf : lst) {
                Field f = morphium.getMapper().getField(type, ctf);
                try {
                    f.set(o, morphium.getConfig().getSecurityMgr().getCurrentUserId());
                } catch (IllegalAccessException e) {
//                    logger.error("Could not set changed by",e);
                }

            }
            //Storing access timestamps
            morphium.store(unmarshall);
        }
    }

    @Override
    public T getById(ObjectId id) {
        List<String> flds = morphium.getMapper().getFields(type, Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new RuntimeException("Type does not have an ID-Field? " + type.getSimpleName());
        }
        //should only be one
        String f = flds.get(0);
        Query<T> q = q().f(f).eq(id); //prepare
        return q.get();
    }

    @Override
    public T get() {
        Cache c = morphium.getAnnotationFromHierarchy(type, Cache.class); //type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        String ck = morphium.getCacheKey(this);
        morphium.inc(StatisticKeys.READS);
        if (readCache) {
            if (morphium.isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                List<T> lst = morphium.getFromCache(type, ck);
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
        DBCollection coll = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(type));
        setReadPreferenceFor(coll);
        List<Field> fldlst = morphium.getMapper().getAllFields(type);
        BasicDBObject fl = new BasicDBObject();
        fl.put("_id", 1);
        for (Field f : fldlst) {
            if (f.isAnnotationPresent(AdditionalData.class)) {
                //to enable additional data
                fl = new BasicDBObject();
                break;
            }
            String n = morphium.getMapper().getFieldName(type, f.getName());
            fl.put(n, 1);
        }

        DBCursor srch = coll.find(toQueryObject(), fl);
        srch.limit(1);
        if (skip != 0) {
            srch = srch.skip(skip);
        }
        if (order != null) {
            BasicDBObject srt = new BasicDBObject();
            for (String k : order.keySet()) {
                srt.append(k, order.get(k));
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
                morphium.addToCache(ck, type, lst);
            }
            return unmarshall;
        }

        if (readCache) {
            morphium.addToCache(ck, type, lst);
        }
        return null;
    }

    @Override
    public List<ObjectId> idList() {
        Cache c = morphium.getAnnotationFromHierarchy(type, Cache.class);//type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        List<ObjectId> ret = new ArrayList<ObjectId>();
        String ck = morphium.getCacheKey(this);
        ck += " idlist";
        morphium.inc(StatisticKeys.READS);
        if (readCache) {

            if (morphium.isCached(type, ck)) {
                morphium.inc(StatisticKeys.CHITS);
                //casts are not nice... any idea how to change that?
                return (List<ObjectId>) morphium.getFromCache(type, ck);
            }
            morphium.inc(StatisticKeys.CMISS);
        } else {
            morphium.inc(StatisticKeys.NO_CACHED_READS);
        }
        long start = System.currentTimeMillis();
        DBCollection collection = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(type));
        setReadPreferenceFor(collection);
        DBCursor query = collection.find(toQueryObject(), new BasicDBObject("_id", 1)); //only get IDs
        if (order != null) {
            query.sort(new BasicDBObject(order));
        }
        if (skip > 0) {
            query.skip(skip);
        }
        if (limit > 0) {
            query.limit(0);
        }
        Iterator<DBObject> it = query.iterator();

        while (it.hasNext()) {
            DBObject o = it.next();
            ret.add((ObjectId) o.get("_id"));
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingReadEvent(this, dur, ReadAccessType.ID_LIST);
        if (readCache) {
            morphium.addToCache(ck, type, ret);
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
            if (order != null) {
                ret.order = new Hashtable<String, Integer>();
                ret.order.putAll(order);
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
    public Query<T> order(Map<String, Integer> n) {
        return sort(n);
    }

    @Override
    public Query<T> order(String... prefixedString) {
        return sort(prefixedString);
    }
}
