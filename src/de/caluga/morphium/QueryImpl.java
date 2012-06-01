package de.caluga.morphium;

import com.mongodb.*;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.LastAccess;
import de.caluga.morphium.annotations.LastAccessBy;
import de.caluga.morphium.annotations.StoreLastAccess;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.secure.Permission;
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
    private ObjectMapper mapper;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private List<Query<T>> norQueries;

    private int limit = 0, skip = 0;
    private Map<String, Integer> order;

    private Morphium morphium;

    public QueryImpl(Morphium m, Class<T> type, ObjectMapper map) {
        this(m);
        this.type = type;
        mapper = map;
        if (mapper.getMorphium() == null) {
            mapper.setMorphium(m);
        }
    }

    public QueryImpl(Morphium m) {
        morphium = m;
        mapper = new ObjectMapperImpl(m);
        andExpr = new Vector<FilterExpression>();
        orQueries = new Vector<Query<T>>();
        norQueries = new Vector<Query<T>>();
    }

    @Override
    public void setType(Class<T> type) {
        this.type = type;
    }

    @Override
    public Query<T> q() {
        return new QueryImpl<T>(morphium, type, mapper);
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

        DBCollection c = morphium.getDatabase().getCollection(morphium.getConfig().getMapper().getCollectionName(type));

        DBCursor cursor = c.find(query);
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
    public ObjectMapper getMapper() {
        return mapper;
    }

    @Override
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void addChild(FilterExpression ex) {
        andExpr.add(ex);
    }

    @Override
    public Query<T> where(String wh) {
        where = wh;
        return getClone();
    }

    public MongoField f(Enum f) {
        return f(f.name());
    }

    @Override
    public MongoField f(String f) {
        String cf = f;
        if (f.contains(".")) {
            //if there is a . only check first part
            cf = f.substring(0, f.indexOf("."));
            //TODO: check field name completely => person.name, check type Person for field name
        }
        Field field = mapper.getField(type, cf);
        if (field == null) {
            throw new IllegalArgumentException("Unknown Field " + f);
        }

        if (field.isAnnotationPresent(Id.class)) {
            f = "_id";
        } else {
            f = mapper.getFieldName(type, f); //handling of aliases
        }
        MongoField<T> fld = morphium.createMongoField(); //new MongoFieldImpl<T>();
        fld.setFieldString(f);
        fld.setMapper(mapper);
        fld.setQuery(this);
        return fld;
    }

    @Override
    public void or(Query<T>... qs) {
        orQueries.addAll(Arrays.asList(qs));
    }

    private Query<T> getClone() {
        return (Query<T>) this;
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

    @Override
    public Query<T> sort(Map<String, Integer> n) {
        order = n;
        return this;
    }

    @Override
    public Query<T> sort(String... prefixedString) {
        Map<String, Integer> m = new HashMap<String, Integer>();
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
            m.put(fld, val);
        }
        return sort(m);
    }

    @Override
    public Query<T> sort(Enum... naturalOrder) {
        Map<String, Integer> m = new HashMap<String, Integer>();
        for (Enum i : naturalOrder) {
            String fld = i.name();
            m.put(fld, 1);
        }
        return sort(m);
    }

    @Override
    public long countAll() {
        if (morphium.accessDenied(type, Permission.READ)) {
            throw new RuntimeException("Access denied!");
        }
        morphium.inc(Morphium.StatisticKeys.READS);
        return morphium.getDatabase().getCollection(mapper.getCollectionName(type)).count(toQueryObject());
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
        morphium.inc(Morphium.StatisticKeys.READS);
        Cache c = type.getAnnotation(Cache.class);
        boolean useCache = c != null && c.readCache();

        String ck = morphium.getCacheKey(this);
        if (useCache) {
            if (morphium.isCached(type, ck)) {
                morphium.inc(Morphium.StatisticKeys.CHITS);
                return morphium.getFromCache(type, ck);
            }
            morphium.inc(Morphium.StatisticKeys.CMISS);
        } else {
            morphium.inc(Morphium.StatisticKeys.NO_CACHED_READS);

        }

        DBCursor query = morphium.getDatabase().getCollection(mapper.getCollectionName(type)).find(toQueryObject());
        if (skip > 0) {
            query.skip(skip);
        }
        if (limit > 0) {
            query.limit(limit);
        }
        if (order != null) {
            query.sort(new BasicDBObject(order));
        }

        Iterator<DBObject> it = query.iterator();
        List<T> ret = new ArrayList<T>();

        while (it.hasNext()) {
            DBObject o = it.next();
            T unmarshall = mapper.unmarshall(type, o);
            ret.add(unmarshall);

            updateLastAccess(o, unmarshall);

            morphium.firePostLoadEvent(unmarshall);
        }


        if (useCache) {
            morphium.addToCache(ck, type, ret);
        }
        return ret;
    }

    private void updateLastAccess(DBObject o, T unmarshall) {
        if (type.isAnnotationPresent(StoreLastAccess.class)) {
            List<String> lst = mapper.getFields(type, LastAccess.class);
            for (String ctf : lst) {
                Field f = mapper.getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(unmarshall, System.currentTimeMillis());
                    } catch (IllegalAccessException e) {
                        System.out.println("Could not set modification time");

                    }
                }
            }
            lst = mapper.getFields(type, LastAccessBy.class);
            for (String ctf : lst) {
                Field f = mapper.getField(type, ctf);
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
        List<String> flds = mapper.getFields(type, Id.class);
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
        Cache c = type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        String ck = morphium.getCacheKey(this);
        morphium.inc(Morphium.StatisticKeys.READS);
        if (readCache) {
            if (morphium.isCached(type, ck)) {
                morphium.inc(Morphium.StatisticKeys.CHITS);
                List<T> lst = morphium.getFromCache(type, ck);
                if (lst == null || lst.isEmpty()) {
                    return null;
                } else {
                    return lst.get(0);
                }

            }
            morphium.inc(Morphium.StatisticKeys.CMISS);
        } else {
            morphium.inc(Morphium.StatisticKeys.NO_CACHED_READS);
        }
        BasicDBObject sort = new BasicDBObject();
        if (order != null) {
            sort.putAll(order);
        }

        DBCursor srch = morphium.getDatabase().getCollection(mapper.getCollectionName(type)).find(toQueryObject()).limit(1);
        if (skip != 0) {
            srch = srch.skip(skip);
        }
        if (sort.size() != 0) {
            srch = srch.sort(sort);
        }
        if (srch.length() == 0) {
            return null;
        }
        DBObject ret = srch.toArray(1).get(0);
        List<T> lst = new ArrayList<T>(1);
        if (ret != null) {
            T unmarshall = mapper.unmarshall(type, ret);
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
        Cache c = type.getAnnotation(Cache.class);
        boolean readCache = c != null && c.readCache();
        List<ObjectId> ret = new ArrayList<ObjectId>();
        String ck = morphium.getCacheKey(this);
        ck += " idlist";
        morphium.inc(Morphium.StatisticKeys.READS);
        if (readCache) {

            if (morphium.isCached(type, ck)) {
                morphium.inc(Morphium.StatisticKeys.CHITS);
                //casts are not nice... any idea how to change that?
                return (List<ObjectId>) morphium.getFromCache(type, ck);
            }
            morphium.inc(Morphium.StatisticKeys.CMISS);
        } else {
            morphium.inc(Morphium.StatisticKeys.NO_CACHED_READS);
        }
        DBCursor query = morphium.getDatabase().getCollection(mapper.getCollectionName(type)).find(toQueryObject(), new BasicDBObject("_id", 1)); //only get IDs
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
        if (readCache) {
            morphium.addToCache(ck, type, ret);
        }
        return ret;
    }


    public Query<T> clone() {
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
