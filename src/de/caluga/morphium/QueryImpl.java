package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.StoreLastAccess;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:14
 * <p/>
 * TODO: Add documentation here
 */
public class QueryImpl<T> implements Query<T>, Cloneable {
    private String where;
    private Class<T> type;
    private ObjectMapper mapper;
    private List<FilterExpression> andExpr;
    private List<Query<T>> orQueries;
    private Query<T> notQuery;
    private List<Query<T>> norQueries;

    private int limit = 0, skip = 0;
    private Map<String, Integer> order;

    public QueryImpl(Class<T> type, ObjectMapper map) {
        this();
        this.type = type;
        mapper = map;
    }

    public QueryImpl() {
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
        return new QueryImpl(type, mapper);
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


    @Override
    public MongoField f(String f) {
        String cf = f;
        if (f.contains(".")) {
            //if there is a . only check first poart
            cf = f.substring(0, f.indexOf("."));
            //TODO: check field name completely => person.name, check type Person for field name
        }
        Field field = mapper.getField(type, cf);
        if (field == null) {
            throw new IllegalArgumentException("Unknown Field " + f);
        }
        if (field.isAnnotationPresent(Id.class)) {
            f = "_id";
        }
        MongoField<T> fld = Morphium.get().createMongoField(); //new MongoFieldImpl<T>();
        fld.setFieldString(f);
        fld.setMapper(mapper);
        fld.setQuery(this);
        return fld;
    }

    @Override
    public void or(Query<T>... qs) {
        for (Query<T> q : qs) {
            orQueries.add(q);
        }
        return;
    }

    private Query<T> getClone() {
        return (Query<T>) this;
    }

    @Override
    public void not(Query<T> q) {
        notQuery = q;
        return;
    }

    @Override
    public void nor(Query<T>... qs) {
        for (Query<T> q : qs) {
            norQueries.add(q);
        }
        return;
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
    public Query<T> order(Map<String, Integer> n) {
        order = n;
        return this;
    }

    @Override
    public Query<T> order(String... prefixedString) {
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
        return order(m);
    }

    @Override
    public long countAll() {
        return Morphium.get().getDatabase().getCollection(mapper.getCollectionName(type)).count(toQueryObject());
    }


    @Override
    public DBObject toQueryObject() {
        BasicDBObject o = new BasicDBObject();
        BasicDBList lst = new BasicDBList();
        if (orQueries.size() != 0) {
            for (Query<T> ex : orQueries) {
                lst.add(ex.toQueryObject());
            }
            o.put("$or", lst);
        } else {
            if (andExpr.size() == 1) {
                return andExpr.get(0).dbObject();
            }
            if (andExpr.isEmpty()) {
                return o;
            }
            for (FilterExpression ex : andExpr) {
                lst.add(ex.dbObject());
            }
            o.put("$and", lst);
        }
        return o;
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public List<T> asList() {
        String ck = Morphium.get().getCacheKey(this);
        if (Morphium.get().isCached(type, ck)) {
            return Morphium.get().getFromCache(type, ck);
        }
        DBCursor query = Morphium.get().getDatabase().getCollection(mapper.getCollectionName(type)).find(toQueryObject());
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
        List<T> ret = new ArrayList<T>();

        while (it.hasNext()) {
            DBObject o = it.next();
            T unmarshall = mapper.unmarshall(type, o);
            ret.add(unmarshall);

            updateLastAccess(o, unmarshall);

            Morphium.get().firePostLoadEvent(unmarshall);
        }
        return ret;
    }

    private void updateLastAccess(DBObject o, T unmarshall) {
        if (type.isAnnotationPresent(StoreLastAccess.class)) {
            StoreLastAccess t = (StoreLastAccess) type.getAnnotation(StoreLastAccess.class);
            String ctf = t.lastAccessField();
            Field f = mapper.getField(type, ctf);
            if (f != null) {
                try {
                    f.set(unmarshall, System.currentTimeMillis());
                } catch (IllegalAccessException e) {
                    System.out.println("Could not set modification time");

                }
                if (t.logUser()) {
                    ctf = t.lastAccessUserField();
                    f = mapper.getField(type, ctf);
                    try {
                        f.set(o, Morphium.getConfig().getSecurityMgr().getCurrentUserId());
                    } catch (IllegalAccessException e) {
//                    logger.error("Could not set changed by",e);
                    }
                }
                //Storing access timestamps
                Morphium.get().store(unmarshall);
            }
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
        Morphium.get().inc(type, StatisticKeys.READS);
        String ck = Morphium.get().getCacheKey(this);
        if (Morphium.get().isCached(type, ck)) {
            Morphium.get().inc(type, StatisticKeys.CHITS);
            List<T> lst = Morphium.get().getFromCache(type, ck);
            if (lst == null || lst.isEmpty()) {
                return null;
            } else {
                return lst.get(0);
            }
        }
        Morphium.get().inc(type, StatisticKeys.CMISS);
        DBObject ret = Morphium.get().getDatabase().getCollection(mapper.getCollectionName(type)).findOne(toQueryObject());
        if (ret != null) {
            T unmarshall = mapper.unmarshall(type, ret);
            Morphium.get().firePostLoadEvent(unmarshall);
            updateLastAccess(ret, unmarshall);
            return unmarshall;
        }
        return null;
    }

    @Override
    public List<ObjectId> idList() {
        Morphium.get().inc(type, StatisticKeys.READS);
        List<ObjectId> ret = new ArrayList<ObjectId>();
        String ck = Morphium.get().getCacheKey(this);
        ck += " idlist";
        if (Morphium.get().isCached(type, ck)) {
            Morphium.get().inc(type, StatisticKeys.CHITS);
            //not nice...
            return (List<ObjectId>) Morphium.get().getFromCache(type, ck);
        }
        Morphium.get().inc(type, StatisticKeys.CMISS);
        DBCursor query = Morphium.get().getDatabase().getCollection(mapper.getCollectionName(type)).find(toQueryObject(), new BasicDBObject("_id", 1)); //only get IDs
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
        return ret;
    }


}
