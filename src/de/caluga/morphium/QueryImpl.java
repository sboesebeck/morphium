package de.caluga.morphium;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.StoreLastAccess;
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
    private Query<T> notQuery;
    private List<Query<T>> norQueries;

    private int limit = 0, skip = 0;
    private Map<String, Integer> order;

    private Morphium morphium;
    
    public QueryImpl(Morphium m,Class<T> type, ObjectMapper map) {
        this(m);
        this.type = type;
        mapper = map;
        if (mapper.getMorphium()==null) {
            mapper.setMorphium(m);
        }
    }

    public QueryImpl(Morphium m) {
        morphium = m;
        mapper=new ObjectMapperImpl(m);
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
        return new QueryImpl<T>(morphium,type, mapper);
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
            f=mapper.getFieldName(type,f); //handling of aliases
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
    public void not(Query<T> q) {
        notQuery = q;
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
        if (morphium.accessDenied(type, Permission.READ)) {
            throw new RuntimeException("Access denied!");
        }
        return morphium.getDatabase().getCollection(mapper.getCollectionName(type)).count(toQueryObject());
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
        }
            if (andExpr.size() == 1 && orQueries.isEmpty()) {
                return andExpr.get(0).dbObject();
            }
            if (andExpr.isEmpty()) {
                return o;
            }

            for (FilterExpression ex : andExpr) {
                lst.add(ex.dbObject());
            }
            o.put("$and", lst);
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

        String ck = morphium.getCacheKey(this);
        if (morphium.isCached(type, ck)) {
            return morphium.getFromCache(type, ck);
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
        morphium.addToCache(ck, type, ret);
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
                        f.set(o, morphium.getConfig().getSecurityMgr().getCurrentUserId());
                    } catch (IllegalAccessException e) {
//                    logger.error("Could not set changed by",e);
                    }
                }
                //Storing access timestamps
                morphium.store(unmarshall);
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
        String ck = morphium.getCacheKey(this);
        if (morphium.isCached(type, ck)) {
            List<T> lst = morphium.getFromCache(type, ck);
            if (lst == null || lst.isEmpty()) {
                return null;
            } else {
                return lst.get(0);
            }
        }
        DBObject ret = morphium.getDatabase().getCollection(mapper.getCollectionName(type)).findOne(toQueryObject());
        List<T> lst = new ArrayList<T>(1);
        if (ret != null) {
            T unmarshall = mapper.unmarshall(type, ret);
            morphium.firePostLoadEvent(unmarshall);
            updateLastAccess(ret, unmarshall);

            lst.add((T) unmarshall);
            morphium.addToCache(ck, type, lst);
            return unmarshall;
        }
        morphium.addToCache(ck, type, lst);
        return null;
    }

    @Override
    public List<ObjectId> idList() {
        List<ObjectId> ret = new ArrayList<ObjectId>();
        String ck = morphium.getCacheKey(this);
        ck += " idlist";
        if (morphium.isCached(type, ck)) {
            //not nice...
            return (List<ObjectId>) morphium.getFromCache(type, ck);
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
        morphium.addToCache(ck, type, ret);
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
            if (notQuery != null) {
                ret.notQuery = notQuery.clone();
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

}
