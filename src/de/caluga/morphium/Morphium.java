/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import com.mongodb.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.annotations.security.NoProtection;
import de.caluga.morphium.cache.CacheElement;
import de.caluga.morphium.cache.CacheHousekeeper;
import de.caluga.morphium.secure.MongoSecurityException;
import de.caluga.morphium.secure.MongoSecurityManager;
import de.caluga.morphium.secure.Permission;
import net.sf.cglib.proxy.Enhancer;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is the single access point for accessing MongoDB. This should
 *
 * @author stephan
 */
public final class Morphium {

    /**
     * singleton is usually not a good idea in j2ee-Context, but as we did it on
     * several places in the Application it's the easiest way Usage:
     * <code>
     * MorphiumConfig cfg=new MorphiumConfig("testdb",false,false,10,5000,2500);
     * cfg.addAddress("localhost",27017);
     * Morphium.config=cfg;
     * Morphium l=Morphium.get();
     * if (l==null) {
     * System.out.println("Error establishing connection!");
     * System.exit(1);
     * }
     * </code>
     *
     * @see MorphiumConfig
     */
    private final static Logger logger = Logger.getLogger(Morphium.class);
    private MorphiumConfig config;
    private Mongo mongo;
    private DB database;
    private ThreadPoolExecutor writers = new ThreadPoolExecutor(10, 50,
            10000L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    //Cache by Type, query String -> CacheElement (contains list etc)
    private Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cache;
    private Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> idCache;
    private final Map<StatisticKeys, StatisticValue> stats;
    private Map<Class<?>, Map<Class<? extends Annotation>, Method>> lifeCycleMethods;
    /**
     * String Representing current user - needs to be set by Application
     */
    private String currentUser;
    private CacheHousekeeper cacheHousekeeper;

    private Vector<MorphiumStorageListener> listeners;
    private Vector<ProfilingListener> profilingListeners;
    private Vector<Thread> privileged;
    private Vector<ShutdownListener> shutDownListeners;

    public MorphiumConfig getConfig() {
        return config;
    }
//    private boolean securityEnabled = false;

    /**
     * init the MongoDbLayer. Uses Morphium-Configuration Object for Configuration.
     * Needs to be set before use or RuntimeException is thrown!
     * all logging is done in INFO level
     *
     * @see MorphiumConfig
     */
    public Morphium(MorphiumConfig cfg) {
        if (cfg == null) {
            throw new RuntimeException("Please specify configuration!");
        }
        config = cfg;
        privileged = new Vector<Thread>();
        shutDownListeners = new Vector<ShutdownListener>();
        listeners = new Vector<MorphiumStorageListener>();
        profilingListeners = new Vector<ProfilingListener>();
        cache = new Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>();
        idCache = new Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>>();

        stats = new Hashtable<StatisticKeys, StatisticValue>();
        lifeCycleMethods = new Hashtable<Class<?>, Map<Class<? extends Annotation>, Method>>();
        for (StatisticKeys k : StatisticKeys.values()) {
            stats.put(k, new StatisticValue());
        }


        //dummyUser.setGroupIds();
        MongoOptions o = new MongoOptions();
        o.autoConnectRetry = true;
        o.fsync = true;
        o.socketTimeout = config.getSocketTimeout();
        o.connectTimeout = config.getConnectionTimeout();
        o.connectionsPerHost = config.getMaxConnections();
        o.socketKeepAlive = config.isSocketKeepAlive();
        o.threadsAllowedToBlockForConnectionMultiplier = 5;
        o.safe = false;

        writers.setCorePoolSize(config.getMaxConnections() / 2);
        writers.setMaximumPoolSize(config.getMaxConnections());

        if (config.getAdr().isEmpty()) {
            throw new RuntimeException("Error - no server address specified!");
        }
        switch (config.getMode()) {
            case REPLICASET:
                if (config.getAdr().size() < 2) {

                    throw new RuntimeException("at least 2 Server Adresses needed for MongoDB in ReplicaSet Mode!");
                }
                mongo = new Mongo(config.getAdr(), o);
                break;
            case PAIRED:
                throw new RuntimeException("PAIRED Mode not available anymore!!!!");
//                if (config.getAdr().size() != 2) {
//                    morphia = null;
//                    dataStore = null;
//                    throw new RuntimeException("2 Server Adresses needed for MongoDB in Paired Mode!");
//                }
//
//                morphium = new Mongo(config.getAdr().get(0), config.getAdr().get(1), o);
//                break;
            case SINGLE:
            default:
                if (config.getAdr().size() > 1) {
//                    Logger.getLogger(Morphium.class.getName()).warning("WARNING: ignoring additional server Adresses only using 1st!");
                }
                mongo = new Mongo(config.getAdr().get(0), o);
                break;
        }

        database = mongo.getDB(config.getDatabase());
        if (config.isSlaveOk()) {
            mongo.setReadPreference(ReadPreference.SECONDARY);
        }
        if (config.getMongoLogin() != null) {
            if (!database.authenticate(config.getMongoLogin(), config.getMongoPassword().toCharArray())) {
                throw new RuntimeException("Authentication failed!");
            }
        }
//        int cnt = database.getCollection("system.indexes").find().count(); //test connection

        if (config.getConfigManager() == null) {
            config.setConfigManager(new ConfigManagerImpl());
        }
        config.getConfigManager().setMorphium(this);
        cacheHousekeeper = new CacheHousekeeper(this, 5000, config.getGlobalCacheValidTime());
        cacheHousekeeper.start();
        config.getConfigManager().startCleanupThread();
        if (config.getMapper() == null) {
            config.setMapper(new ObjectMapperImpl(this));
        } else {
            config.getMapper().setMorphium(this);
        }
        logger.info("Initialization successful...");

    }

    public void addListener(MorphiumStorageListener lst) {
        listeners.add(lst);
    }

    public void removeListener(MorphiumStorageListener lst) {
        listeners.remove(lst);
    }


    public Mongo getMongo() {
        return mongo;
    }

    public DB getDatabase() {
        return database;
    }


    public ConfigManager getConfigManager() {
        return config.getConfigManager();
    }

    /**
     * search for objects similar to template concerning all given fields.
     * If no fields are specified, all NON Null-Fields are taken into account
     * if specified, field might also be null
     *
     * @param template
     * @param fields
     * @param <T>
     * @return
     */
    public <T> List<T> findByTemplate(T template, String... fields) {
        Class cls = template.getClass();
        List<String> flds = new ArrayList<String>();
        if (fields.length > 0) {
            flds.addAll(Arrays.asList(fields));
        } else {
            flds = getFields(cls);
        }
        Query<T> q = createQueryFor(cls);
        for (String f : flds) {
            try {
                q.f(f).eq(getValue(template, f));
            } catch (Exception e) {
                logger.error("Could not read field " + f + " of object " + cls.getName());
            }
        }
        return q.asList();
    }

    public void unset(Object toSet, Enum field) {
        unset(toSet, field.name());
    }

    /**
     * can be called for autmatic index ensurance. Attention: might cause heavy load on mongo
     * will be called automatically if a new collection is created
     *
     * @param type
     */
    public void ensureIndicesFor(Class type) {
        if (isAnnotationPresentInHierarchy(type, Index.class)) {
            //type must be marked as to be indexed
            List<Annotation> lst = getAllAnnotationsFromHierachy(type, Index.class);
            for (Annotation a : lst) {
                Index i = (Index) a;
                if (i.value().length > 0) {
                    for (String idx : i.value()) {
                        String[] idxStr = idx.replaceAll(" +", "").split(",");
                        ensureIndex(type, idxStr);
                    }
                }
            }

            List<String> flds = config.getMapper().getFields(type, Index.class);
            if (flds != null && flds.size() > 0) {
                for (String f : flds) {
                    Index i = config.getMapper().getField(type, f).getAnnotation(Index.class);
                    if (i.decrement()) {
                        ensureIndex(type, "-" + f);
                    } else {
                        ensureIndex(type, f);
                    }
                }
            }
        }
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    public void unset(Object toSet, String field) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
        }
        Class cls = toSet.getClass();
        firePreUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

        String coll = config.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", getId(toSet));
        Field f = getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$unset", new BasicDBObject(fieldName, 1));
        WriteConcern wc = getWriteConcernForClass(toSet.getClass());
        if (!database.collectionExists(coll)) {
            ensureIndicesFor(cls);
        }
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(coll).update(query, update);
        } else {
            database.getCollection(coll).update(query, update, false, false, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(cls);
        try {
            f.set(toSet, null);
        } catch (IllegalAccessException e) {
            //May happen, if null is not allowed for example
        }
        firePostUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

    }


    private void clearCacheIfNecessary(Class cls) {
        Cache c = getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
        if (c != null) {
            if (c.clearOnWrite()) {
                clearCachefor(cls);
            }
        }
    }

    private DBObject simplifyQueryObject(DBObject q) {
        if (q.keySet().size() == 1 && q.get("$and") != null) {
            BasicDBObject ret = new BasicDBObject();
            BasicDBList lst = (BasicDBList) q.get("$and");
            for (Object o : lst) {
                if (o instanceof DBObject) {
                    ret.putAll(((DBObject) o));
                } else if (o instanceof Map) {
                    ret.putAll(((Map) o));
                } else {
                    //something we cannot handle
                    return q;
                }
            }
            return ret;
        }
        return q;
    }

    public void set(Query<?> query, Enum field, Object val) {
        set(query, field.name(), val);
    }

    public void set(Query<?> query, String field, Object val) {
        set(query, field, val, false, false);
    }

    public void setEnum(Query<?> query, Map<Enum, Object> values, boolean insertIfNotExist, boolean multiple) {
        HashMap<String, Object> toSet = new HashMap<String, Object>();
        for (Map.Entry<Enum, Object> est : values.entrySet()) {
            toSet.put(est.getKey().name(), values.get(est.getValue()));
        }
        set(query, toSet, insertIfNotExist, multiple);
    }

    public void push(Query<?> query, Enum field, Object value) {
        pushPull(true, query, field.name(), value, false, true);
    }

    public void pull(Query<?> query, Enum field, Object value) {
        pushPull(false, query, field.name(), value, false, true);
    }

    public void push(Query<?> query, String field, Object value) {
        pushPull(true, query, field, value, false, true);
    }

    public void pull(Query<?> query, String field, Object value) {
        pushPull(false, query, field, value, false, true);
    }


    public void push(Query<?> query, Enum field, Object value, boolean insertIfNotExist, boolean multiple) {
        pushPull(true, query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pull(Query<?> query, Enum field, Object value, boolean insertIfNotExist, boolean multiple) {
        pushPull(false, query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pushAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pushPullAll(true, query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pullAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pushPullAll(false, query, field.name(), value, insertIfNotExist, multiple);
    }


    public void push(Query<?> query, String field, Object value, boolean insertIfNotExist, boolean multiple) {
        pushPull(true, query, field, value, insertIfNotExist, multiple);
    }

    public void pull(Query<?> query, String field, Object value, boolean insertIfNotExist, boolean multiple) {
        pushPull(false, query, field, value, insertIfNotExist, multiple);
    }

    public void pushAll(Query<?> query, String field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pushPullAll(true, query, field, value, insertIfNotExist, multiple);
    }

    public void pullAll(Query<?> query, String field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pushPull(false, query, field, value, insertIfNotExist, multiple);
    }


    private void pushPull(boolean push, Query<?> query, String field, Object value, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        firePreUpdateEvent(getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        String coll = config.getMapper().getCollectionName(cls);

        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = simplifyQueryObject(qobj);
        }
        field = config.getMapper().getFieldName(cls, field);
        BasicDBObject set = new BasicDBObject(field, value);
        BasicDBObject update = new BasicDBObject(push ? "$push" : "$pull", set);

        pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);

    }

    private void pushIt(boolean push, boolean insertIfNotExist, boolean multiple, Class<?> cls, String coll, DBObject qobj, BasicDBObject update) {
        if (!database.collectionExists(coll) && insertIfNotExist) {
            ensureIndicesFor(cls);
        }
        WriteConcern wc = getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(cls);
        firePostUpdateEvent(getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
    }

    private void pushPullAll(boolean push, Query<?> query, String field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        String coll = config.getMapper().getCollectionName(cls);
        firePreUpdateEvent(getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        BasicDBList dbl = new BasicDBList();
        dbl.addAll(value);

        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = simplifyQueryObject(qobj);
        }
        field = config.getMapper().getFieldName(cls, field);
        BasicDBObject set = new BasicDBObject(field, value);
        BasicDBObject update = new BasicDBObject(push ? "$pushAll" : "$pullAll", set);
        pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);
    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query            - query to specify which objects should be set
     * @param values           - map fieldName->Value, which values are to be set!
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple         - update several documents, if false, only first hit will be updated
     */
    public void set(Query<?> query, Map<String, Object> values, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        String coll = config.getMapper().getCollectionName(cls);
        firePreUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
        BasicDBObject toSet = new BasicDBObject();
        for (Map.Entry<String, Object> ef : values.entrySet()) {
            String fieldName = getFieldName(cls, ef.getKey());
            toSet.put(fieldName, ef.getValue());
        }
        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = simplifyQueryObject(qobj);
        }

        if (insertIfNotExist && !database.collectionExists(coll)) {
            ensureIndicesFor(cls);
        }

        BasicDBObject update = new BasicDBObject("$set", toSet);
        WriteConcern wc = getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(cls);
        firePostUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query            - query to specify which objects should be set
     * @param field            - field to set
     * @param val              - value to set
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple         - update several documents, if false, only first hit will be updated
     */
    public void set(Query<?> query, String field, Object val, boolean insertIfNotExist, boolean multiple) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(field, val);
        set(query, map, insertIfNotExist, multiple);
    }

    public void dec(Query<?> query, Enum field, int amount, boolean insertIfNotExist, boolean multiple) {
        dec(query, field.name(), amount, insertIfNotExist, multiple);
    }

    public void dec(Query<?> query, String field, int amount, boolean insertIfNotExist, boolean multiple) {
        inc(query, field, -amount, insertIfNotExist, multiple);
    }

    public void dec(Query<?> query, String field, int amount) {
        inc(query, field, -amount, false, false);
    }

    public void dec(Query<?> query, Enum field, int amount) {
        inc(query, field, -amount, false, false);
    }

    public void inc(Query<?> query, String field, int amount) {
        inc(query, field, amount, false, false);
    }

    public void inc(Query<?> query, Enum field, int amount) {
        inc(query, field, amount, false, false);
    }

    public void inc(Query<?> query, Enum field, int amount, boolean insertIfNotExist, boolean multiple) {
        inc(query, field.name(), amount, insertIfNotExist, multiple);
    }

    public void inc(Query<?> query, String field, int amount, boolean insertIfNotExist, boolean multiple) {
        Class<?> cls = query.getType();
        firePreUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
        String coll = config.getMapper().getCollectionName(cls);
        String fieldName = getFieldName(cls, field);
        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
        DBObject qobj = query.toQueryObject();
        if (insertIfNotExist) {
            qobj = simplifyQueryObject(qobj);
        }
        if (insertIfNotExist && !database.collectionExists(coll)) {
            ensureIndicesFor(cls);
        }
        WriteConcern wc = getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
        } else {
            database.getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(cls);
        firePostUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
    }


    public void set(Object toSet, Enum field, Object value) {
        set(toSet, field.name(), value);
    }

    /**
     * setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$set:{field:value}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: the field to change
     * @param value: the value to set
     */
    public void set(Object toSet, String field, Object value) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            storeNoCache(toSet);
        }
        Class cls = toSet.getClass();
        firePreUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
        String coll = config.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", getId(toSet));
        Field f = getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject(fieldName, value));


        WriteConcern wc = getWriteConcernForClass(toSet.getClass());
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(coll).update(query, update);
        } else {
            database.getCollection(coll).update(query, update, false, false, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, update, dur, false, WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(cls);
        try {
            f.set(toSet, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        firePostUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);

    }

    /**
     * decreasing a value of a given object
     * calles <code>inc(toDec,field,-amount);</code>
     */
    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    public void inc(Object toInc, String field, int amount) {
        if (toInc == null) throw new RuntimeException("Cannot update null!");
        if (getId(toInc) == null) {
            logger.info("just storing object as it is new...");
            storeNoCache(toInc);
        }
        Class cls = toInc.getClass();
        firePreUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
        String coll = config.getMapper().getCollectionName(cls);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", getId(toInc));
        Field f = getField(cls, field);
        if (f == null) {
            throw new RuntimeException("Unknown field: " + field);
        }
        String fieldName = getFieldName(cls, field);

        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(fieldName, amount));
        WriteConcern wc = getWriteConcernForClass(toInc.getClass());
        if (wc == null) {
            database.getCollection(coll).update(query, update);
        } else {
            database.getCollection(coll).update(query, update, false, false, wc);
        }

        clearCacheIfNecessary(cls);

        //TODO: check inf necessary
        if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
            try {
                f.set(toInc, ((Integer) f.get(toInc)) + (int) amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
            try {
                f.set(toInc, ((Double) f.get(toInc)) + amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
            try {
                f.set(toInc, ((Float) f.get(toInc)) + (float) amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
            try {
                f.set(toInc, ((Long) f.get(toInc)) + (long) amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.error("Could not set increased value - unsupported type " + cls.getName());
        }
        firePostUpdateEvent(getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);


    }

    public void setIdCache(Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> c) {
        idCache = c;
    }

    /**
     * adds some list of objects to the cache manually...
     * is being used internally, and should be used with care
     *
     * @param k    - Key, usually the mongodb query string
     * @param type - class type
     * @param ret  - list of results
     * @param <T>  - Type of record
     */
    public <T extends Object> void addToCache(String k, Class<? extends Object> type, List<T> ret) {
        if (k == null) {
            return;
        }
        if (ret != null) {
            //copy from idCache
            Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> idCacheClone = cloneIdCache();
            for (T record : ret) {
                if (idCacheClone.get(type) == null) {
                    idCacheClone.put(type, new Hashtable<ObjectId, Object>());
                }
                idCacheClone.get(type).put(config.getMapper().getId(record), record);
            }
            setIdCache(idCacheClone);
        }

        CacheElement e = new CacheElement(ret);
        e.setLru(System.currentTimeMillis());
        Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cl = (Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>) cache.clone();
        if (cl.get(type) == null) {
            cl.put(type, new Hashtable<String, CacheElement>());
        }
        cl.get(type).put(k, e);

        //atomar execution of this operand - no synchronization needed
        cache = cl;

    }

    protected void setPrivilegedThread(Thread thr) {

    }


    protected void inc(StatisticKeys k) {
        stats.get(k).inc();
    }


    public String toJsonString(Object o) {
        return config.getMapper().marshall(o).toString();
    }


    public int writeBufferCount() {
        return writers.getQueue().size();
    }


    public String getCacheKey(DBObject qo, Map<String, Integer> sort, int skip, int limit) {
        StringBuffer b = new StringBuffer();
        b.append(qo.toString());
        b.append(" l:");
        b.append(limit);
        b.append(" s:");
        b.append(skip);
        if (sort != null) {
            b.append(" sort:");
            b.append(new BasicDBObject(sort).toString());
        }
        return b.toString();
    }

    /**
     * create unique cache key for queries, also honoring skip & limit and sorting
     *
     * @param q
     * @return
     */
    public String getCacheKey(Query q) {
        return getCacheKey(q.toQueryObject(), q.getOrder(), q.getSkip(), q.getLimit());
    }


    private void storeNoCacheUsingFields(Object ent, String... fields) {
        ObjectId id = getId(ent);
        if (ent == null) return;
        if (id == null) {
            //new object - update not working
            logger.warn("trying to partially update new object - storing it in full!");
            storeNoCache(ent);
            return;
        }
        firePreStoreEvent(ent, false);
        inc(StatisticKeys.WRITES);
        DBObject find = new BasicDBObject();

        find.put("_id", id);
        DBObject update = new BasicDBObject();
        for (String f : fields) {
            try {
                Object value = getValue(ent, f);
                if (isAnnotationPresentInHierarchy(value.getClass(), Entity.class)) {
                    value = config.getMapper().marshall(value);
                }
                update.put(f, value);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Class<?> type = getRealClass(ent.getClass());

        StoreLastChange t = getAnnotationFromHierarchy(type, StoreLastChange.class); //(StoreLastChange) type.getAnnotation(StoreLastChange.class);
        if (t != null) {
            List<String> lst = config.getMapper().getFields(ent.getClass(), LastChange.class);

            long now = System.currentTimeMillis();
            for (String ctf : lst) {
                Field f = getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(ent, now);
                    } catch (IllegalAccessException e) {
                        logger.error("Could not set modification time", e);

                    }
                }
                update.put(ctf, now);
            }
            lst = config.getMapper().getFields(ent.getClass(), LastChangeBy.class);
            if (lst != null && lst.size() != 0) {
                for (String ctf : lst) {

                    Field f = getField(type, ctf);
                    if (f != null) {
                        try {
                            f.set(ent, config.getSecurityMgr().getCurrentUserId());
                        } catch (IllegalAccessException e) {
                            logger.error("Could not set changed by", e);
                        }
                    }
                    update.put(ctf, config.getSecurityMgr().getCurrentUserId());
                }
            }
        }


        update = new BasicDBObject("$set", update);
        WriteConcern wc = getWriteConcernForClass(type);
        long start = System.currentTimeMillis();
        if (wc != null) {
            database.getCollection(config.getMapper().getCollectionName(ent.getClass())).update(find, update, false, false, wc);
        } else {
            database.getCollection(config.getMapper().getCollectionName(ent.getClass())).update(find, update, false, false);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
        clearCacheIfNecessary(getRealClass(ent.getClass()));
        firePostStoreEvent(ent, false);
    }

    /**
     * updating an enty in DB without sending the whole entity
     * only transfers the fields to be changed / set
     *
     * @param ent
     * @param fields
     */
    public void updateUsingFields(final Object ent, final String... fields) {
        if (ent == null) return;
        if (fields.length == 0) return; //not doing an update - no change
        if (!isAnnotationPresentInHierarchy(ent.getClass(), NoProtection.class)) {
            if (getId(ent) == null) {
                if (accessDenied(ent, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(ent, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }
        }

        if (isAnnotationPresentInHierarchy(ent.getClass(), NoCache.class)) {
            storeNoCacheUsingFields(ent, fields);
            return;
        }

        Cache cc = getAnnotationFromHierarchy(ent.getClass(), Cache.class); //ent.getClass().getAnnotation(Cache.class);
        if (cc != null) {
            if (cc.writeCache()) {
                writers.execute(new Runnable() {
                    @Override
                    public void run() {
                        storeNoCacheUsingFields(ent, fields);
                    }
                });
                inc(StatisticKeys.WRITES_CACHED);

            } else {
                storeNoCacheUsingFields(ent, fields);
            }
        } else {
            storeNoCacheUsingFields(ent, fields);
        }


    }

    public List<Annotation> getAllAnnotationsFromHierachy(Class<?> cls, Class<? extends Annotation>... anCls) {
        cls = getRealClass(cls);
        List<Annotation> ret = new ArrayList<Annotation>();
        Class<?> z = cls;
        while (!z.equals(Object.class)) {
            if (z.getAnnotations() != null && z.getAnnotations().length != 0) {
                if (anCls.length == 0) {
                    ret.addAll(Arrays.asList(z.getAnnotations()));
                } else {
                    for (Annotation a : z.getAnnotations()) {
                        for (Class<? extends Annotation> ac : anCls) {
                            if (a.annotationType().equals(ac)) {
                                ret.add(a);
                            }
                        }
                    }
                }
            }
            z = z.getSuperclass();

            if (z == null) break;
        }

        return ret;
    }

    /**
     * returns annotations, even if in class hierarchy or
     * lazyloading proxy
     *
     * @param cls
     * @return
     */
    public <T extends Annotation> T getAnnotationFromHierarchy(Class<?> cls, Class<T> anCls) {
        cls = getRealClass(cls);
        if (cls.isAnnotationPresent(anCls)) {
            return cls.getAnnotation(anCls);
        }
        //class hierarchy?
        Class<?> z = cls;
        while (!z.equals(Object.class)) {
            if (z.isAnnotationPresent(anCls)) {
                return z.getAnnotation(anCls);
            }
            z = z.getSuperclass();
            if (z == null) break;
        }
        return null;
    }

    private Class<?> getRealClass(Class<?> cls) {
        return config.getMapper().getRealClass(cls);
    }

    private <T> T getRealObject(T o) {
        return config.getMapper().getRealObject(o);
    }

    public <T extends Annotation> boolean isAnnotationPresentInHierarchy(Class<?> cls, Class<T> anCls) {
        return getAnnotationFromHierarchy(cls, anCls) != null;
    }


    public void callLifecycleMethod(Class<? extends Annotation> type, Object on) {
        if (on == null) return;
        //No synchronized block - might cause the methods to be put twice into the
        //hashtabel - but for performance reasons, it's ok...
        Class<?> cls = on.getClass();
        //No Lifecycle annotation - no method calling
        if (!isAnnotationPresentInHierarchy(cls, Lifecycle.class)) {//cls.isAnnotationPresent(Lifecycle.class)) {
            return;
        }
        //Already stored - should not change during runtime
        if (lifeCycleMethods.get(cls) != null) {
            if (lifeCycleMethods.get(cls).get(type) != null) {
                try {
                    lifeCycleMethods.get(cls).get(type).invoke(on);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
            return;
        }

        Map<Class<? extends Annotation>, Method> methods = new HashMap<Class<? extends Annotation>, Method>();
        //Methods must be public
        for (Method m : cls.getMethods()) {
            for (Annotation a : m.getAnnotations()) {
                methods.put(a.annotationType(), m);
            }
        }
        lifeCycleMethods.put(cls, methods);
        if (methods.get(type) != null) {
            try {
                methods.get(type).invoke(on);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * careful this actually changes the parameter o!
     *
     * @param o
     * @param <T>
     * @return
     */
    public <T> T reread(T o) {
        if (o == null) throw new RuntimeException("Cannot re read null!");
        ObjectId id = getId(o);
        if (id == null) {
            return null;
        }
        DBCollection col = database.getCollection(getConfig().getMapper().getCollectionName(o.getClass()));
        BasicDBObject srch = new BasicDBObject("_id", id);
        DBCursor crs = col.find(srch).limit(1);
        if (crs.hasNext()) {
            DBObject dbo = crs.next();
            Object fromDb = getConfig().getMapper().unmarshall(o.getClass(), dbo);
            List<String> flds = getFields(o.getClass());
            for (String f : flds) {
                Field fld = getConfig().getMapper().getField(o.getClass(), f);
                try {
                    fld.set(o, fld.get(fromDb));
                } catch (IllegalAccessException e) {
                    logger.error("Could not set Value: " + fld);
                }
            }

        } else {
            logger.info("Did not find object with id " + id);
            return null;
        }
        return o;
    }

    private void firePreStoreEvent(Object o, boolean isNew) {
        if (o == null) return;
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.preStore(o, isNew);
        }
        callLifecycleMethod(PreStore.class, o);

    }

    private void firePostStoreEvent(Object o, boolean isNew) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postStore(o, isNew);
        }
        callLifecycleMethod(PostStore.class, o);
        //existing object  => store last Access, if needed

    }

    private void firePreDropEvent(Class cls) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.preDrop(cls);
        }

    }

    private void firePostDropEvent(Class cls) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postDrop(cls);
        }
    }

    private void firePostUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postUpdate(cls, t);
        }
    }

    private void firePreUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.preUpdate(cls, t);
        }
    }

    private void firePostRemoveEvent(Object o) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postRemove(o);
        }
        callLifecycleMethod(PostRemove.class, o);
    }

    private void firePostRemoveEvent(Query q) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postRemove(q);
        }
        //TODO: FIX - Cannot call lifecycle method here

    }

    private void firePreRemoveEvent(Object o) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.preDelete(o);
        }
        callLifecycleMethod(PreRemove.class, o);
    }

    private void firePreRemoveEvent(Query q) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.preRemove(q);
        }
        //TODO: Fix - cannot call lifecycle method
    }

//    private void firePreListStoreEvent(List records, Map<Object,Boolean> isNew) {
//        //Avoid concurrent modification exception
//        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
//        for (MorphiumStorageListener l : lst) {
//            l.preListStore(records,isNew);
//        }
//        for (Object o : records) {
//            callLifecycleMethod(PreStore.class, o);
//        }
//    }

//    private void firePostListStoreEvent(List records, Map<Object,Boolean> isNew) {
//        //Avoid concurrent modification exception
//        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
//        for (MorphiumStorageListener l : lst) {
//            l.postListStore(records,isNew);
//        }
//        for (Object o : records) {
//            callLifecycleMethod(PostStore.class, o);
//        }
//
//    }

    /**
     * will be called by query after unmarshalling
     *
     * @param o
     */
    protected void firePostLoadEvent(Object o) {
        //Avoid concurrent modification exception
        List<MorphiumStorageListener> lst = (List<MorphiumStorageListener>) listeners.clone();
        for (MorphiumStorageListener l : lst) {
            l.postLoad(o);
        }
        callLifecycleMethod(PostLoad.class, o);

    }

    public void storeNoCache(Object o) {
        long start = System.currentTimeMillis();
        Class type = getRealClass(o.getClass());
        if (!isAnnotationPresentInHierarchy(type, Entity.class)) {
            throw new RuntimeException("Not an entity! Storing not possible!");
        }
        inc(StatisticKeys.WRITES);
        ObjectId id = config.getMapper().getId(o);
        if (isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
            if ((o instanceof PartiallyUpdateable)) {
                updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                ((PartiallyUpdateable) o).clearAlteredFields();

                return;
            }
        }
        o = getRealObject(o);
        if (o == null) {
            logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
            return;
        }
        boolean isNew = id == null;
        firePreStoreEvent(o, isNew);
        long dur = System.currentTimeMillis() - start;
        DBObject marshall = config.getMapper().marshall(o);

        if (isNew) {
            //new object - need to store creation time
            if (isAnnotationPresentInHierarchy(type, StoreCreationTime.class)) {
                List<String> lst = config.getMapper().getFields(type, CreationTime.class);
                if (lst == null || lst.size() == 0) {
                    logger.error("Unable to store creation time as @CreationTime is missing");
                } else {
                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Field f = getField(type, ctf);
                        if (f != null) {
                            try {
                                f.set(o, now);
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set creation time", e);

                            }
                        }
                        marshall.put(ctf, now);
                    }

                }
                lst = config.getMapper().getFields(type, CreatedBy.class);
                if (lst != null && lst.size() > 0) {
                    for (String ctf : lst) {

                        Field f = getField(type, ctf);
                        if (f != null) {
                            try {
                                f.set(o, config.getSecurityMgr().getCurrentUserId());
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set created by", e);
                            }
                        }
                        marshall.put(ctf, config.getSecurityMgr().getCurrentUserId());
                    }
                }
            }
        }
        if (isAnnotationPresentInHierarchy(type, StoreLastChange.class)) {
            List<String> lst = config.getMapper().getFields(type, LastChange.class);
            if (lst != null && lst.size() > 0) {
                for (String ctf : lst) {
                    long now = System.currentTimeMillis();
                    Field f = getField(type, ctf);
                    if (f != null) {
                        try {
                            f.set(o, now);
                        } catch (IllegalAccessException e) {
                            logger.error("Could not set modification time", e);

                        }
                    }
                    marshall.put(ctf, now);
                }
            } else {
                logger.warn("Could not store last change - @LastChange missing!");
            }

            lst = config.getMapper().getFields(type, LastChangeBy.class);
            if (lst != null && lst.size() > 0) {
                for (String ctf : lst) {

                    Field f = getField(type, ctf);
                    if (f != null) {
                        try {
                            f.set(o, config.getSecurityMgr().getCurrentUserId());
                        } catch (IllegalAccessException e) {
                            logger.error("Could not set changed by", e);
                        }
                    }
                    marshall.put(ctf, config.getSecurityMgr().getCurrentUserId());
                }
            }
        }

        String coll = config.getMapper().getCollectionName(type);
        if (!database.collectionExists(coll)) {
            if (logger.isDebugEnabled())
                logger.debug("Collection does not exist - ensuring indices");
            ensureIndicesFor(type);
        }

        WriteConcern wc = getWriteConcernForClass(type);
        if (wc != null) {
            database.getCollection(coll).save(marshall, wc);
        } else {

            database.getCollection(coll).save(marshall);
        }
        dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
        if (logger.isDebugEnabled()) {
            String n = "";
            if (isNew) {
                n = "NEW ";
            }
            logger.debug(n + "stored " + type.getSimpleName() + " after " + dur + " ms length:" + marshall.toString().length());
        }
        if (isNew) {
            List<String> flds = config.getMapper().getFields(o.getClass(), Id.class);
            if (flds == null) {
                throw new RuntimeException("Object does not have an ID field!");
            }
            try {
                //Setting new ID (if object was new created) to Entity
                getField(o.getClass(), flds.get(0)).set(o, marshall.get("_id"));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        Cache ch = getAnnotationFromHierarchy(o.getClass(), Cache.class);
        if (ch != null) {
            if (ch.clearOnWrite()) {
                clearCachefor(o.getClass());
            }
        }

        firePostStoreEvent(o, isNew);
    }

    private WriteConcern getWriteConcernForClass(Class<?> cls) {
        WriteSafety safety = getAnnotationFromHierarchy(cls, WriteSafety.class);  // cls.getAnnotation(WriteSafety.class);
        if (safety == null) return null;
        return new WriteConcern(safety.level().getValue(), safety.timeout(), safety.waitForSync(), safety.waitForJournalCommit());
    }

    public void addProfilingListener(ProfilingListener l) {
        profilingListeners.add(l);
    }

    public void removeProfilingListener(ProfilingListener l) {
        profilingListeners.remove(l);
    }

    private void fireProfilingWriteEvent(Class type, Object data, long time, boolean isNew, WriteAccessType wt) {
        for (ProfilingListener l : profilingListeners) {
            try {
                l.writeAccess(type, data, time, isNew, wt);
            } catch (Throwable e) {
                logger.error("Error during profiling: ", e);
            }
        }
    }

    public void fireProfilingReadEvent(Query q, long time, ReadAccessType t) {
        for (ProfilingListener l : profilingListeners) {
            try {
                l.readAccess(q, time, t);
            } catch (Throwable e) {
                logger.error("Error during profiling", e);
            }
        }
    }

    public void storeNoCacheList(List lst) {

        if (!lst.isEmpty()) {
            HashMap<Class, List<Object>> sorted = new HashMap<Class, List<Object>>();
            HashMap<Object, Boolean> isNew = new HashMap<Object, Boolean>();
            for (Object o : lst) {
                Class type = getRealClass(o.getClass());
                if (!isAnnotationPresentInHierarchy(type, Entity.class)) {
                    logger.error("Not an entity! Storing not possible! Even not in list!");
                    continue;
                }
                inc(StatisticKeys.WRITES);
                ObjectId id = config.getMapper().getId(o);
                if (isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                    //not part of list, acutally...
                    if ((o instanceof PartiallyUpdateable)) {
                        updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
                        ((PartiallyUpdateable) o).clearAlteredFields();
                        continue;
                    }
                }
                o = getRealObject(o);
                if (o == null) {
                    logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                    return;
                }

                if (sorted.get(o.getClass()) == null) {
                    sorted.put(o.getClass(), new ArrayList<Object>());
                }
                sorted.get(o.getClass()).add(o);
                if (getId(o) == null) {
                    isNew.put(o, true);
                } else {
                    isNew.put(o, false);
                }
                firePreStoreEvent(o, isNew.get(o));
            }

//            firePreListStoreEvent(lst,isNew);
            for (Map.Entry<Class, List<Object>> es : sorted.entrySet()) {
                Class c = es.getKey();
                ArrayList<DBObject> dbLst = new ArrayList<DBObject>();
                //bulk insert... check if something already exists
                WriteConcern wc = getWriteConcernForClass(c);
                DBCollection collection = database.getCollection(getConfig().getMapper().getCollectionName(c));
                for (Object record : es.getValue()) {
                    DBObject marshall = config.getMapper().marshall(record);
                    if (isNew.get(record)) {
                        dbLst.add(marshall);
                    } else {
                        //single update
                        long start = System.currentTimeMillis();
                        if (wc == null) {
                            collection.save(marshall);
                        } else {
                            collection.save(marshall, wc);
                        }
                        long dur = System.currentTimeMillis() - start;
                        fireProfilingWriteEvent(c, marshall, dur, false, WriteAccessType.SINGLE_INSERT);
                        firePostStoreEvent(record, isNew.get(record));
                    }

                }
                long start = System.currentTimeMillis();
                if (wc == null) {
                    collection.insert(dbLst);
                } else {
                    collection.insert(dbLst, wc);
                }
                long dur = System.currentTimeMillis() - start;
                //bulk insert
                fireProfilingWriteEvent(c, dbLst, dur, true, WriteAccessType.BULK_INSERT);
                for (Object record : es.getValue()) {
                    if (isNew.get(record)) {
                        firePostStoreEvent(record, isNew.get(record));
                    }
                }
            }
//            firePostListStoreEvent(lst,isNew);
        }
    }


    protected boolean isCached(Class<? extends Object> type, String k) {
        Cache c = getAnnotationFromHierarchy(type, Cache.class); ///type.getAnnotation(Cache.class);
        if (c != null) {
            if (!c.readCache()) return false;
        } else {
            return false;
        }
        return cache.get(type) != null && cache.get(type).get(k) != null && cache.get(type).get(k).getFound() != null;
    }

    /**
     * return object by from cache. Cache key usually is the string-representation of the search
     * query.toQueryObject()
     *
     * @param type
     * @param k
     * @param <T>
     * @return
     */
    public <T> List<T> getFromCache(Class<T> type, String k) {
        if (cache.get(type) == null || cache.get(type).get(k) == null) return null;
        final CacheElement cacheElement = cache.get(type).get(k);
        cacheElement.setLru(System.currentTimeMillis());
        return cacheElement.getFound();
    }

    public Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cloneCache() {
        return (Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>) cache.clone();
    }

    public Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> cloneIdCache() {
        return (Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>>) idCache.clone();
    }

    /**
     * issues a delete command - no lifecycle methods calles, no drop, keeps all indexec this way
     *
     * @param cls
     */
    public void clearCollection(Class<? extends Object> cls) {
        delete(createQueryFor(cls));
    }

    /**
     * clears every single object in collection - reads ALL objects to do so
     * this way Lifecycle methods can be called!
     *
     * @param cls
     */

    public void deleteCollectionItems(Class<? extends Object> cls) {
        if (!isAnnotationPresentInHierarchy(cls, NoProtection.class)) { //cls.isAnnotationPresent(NoProtection.class)) {
            try {
                if (accessDenied(cls.newInstance(), Permission.DROP)) {
                    throw new SecurityException("Drop of Collection denied!");
                }
            } catch (InstantiationException ex) {
                Logger.getLogger(Morphium.class).error(ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Morphium.class).error(ex);
            }
        }


        inc(StatisticKeys.WRITES);
        List<? extends Object> lst = readAll(cls);
        for (Object r : lst) {
            deleteObject(r);
        }

        clearCacheIfNecessary(cls);


    }

    /**
     * return a list of all elements stored in morphium for this type
     *
     * @param cls - type to search for, needs to be an Property
     * @param <T> - Type
     * @return - list of all elements stored
     */
    public <T> List<T> readAll(Class<T> cls) {
        inc(StatisticKeys.READS);
        Query<T> qu;
        qu = createQueryFor(cls);
        return qu.asList();
    }

    public <T> Query<T> createQueryFor(Class<T> type) {
        return new QueryImpl<T>(this, type, config.getMapper());
    }

    public <T> List<T> find(Query<T> q) {
        return q.asList();
    }

    private <T> T getFromIDCache(Class<T> type, ObjectId id) {
        if (idCache.get(type) != null) {
            return (T) idCache.get(type).get(id);
        }
        return null;
    }


    public List<Object> distinct(Enum key, Class c) {
        return distinct(key.name(), c);
    }

    /**
     * returns a distinct list of values of the given collection
     * Attention: these values are not unmarshalled, you might get MongoDBObjects
     */
    public List<Object> distinct(Enum key, Query q) {
        return distinct(key.name(), q);
    }

    /**
     * returns a distinct list of values of the given collection
     * Attention: these values are not unmarshalled, you might get MongoDBObjects
     */
    public List<Object> distinct(String key, Query q) {
        return database.getCollection(config.getMapper().getCollectionName(q.getType())).distinct(key, q.toQueryObject());
    }

    public List<Object> distinct(String key, Class cls) {
        return database.getCollection(config.getMapper().getCollectionName(cls)).distinct(key, new BasicDBObject());
    }

    public DBObject group(Query q, Map<String, Object> initial, String jsReduce, String jsFinalize, String... keys) {
        BasicDBObject k = new BasicDBObject();
        BasicDBObject ini = new BasicDBObject();
        ini.putAll(initial);
        for (String ks : keys) {
            if (ks.startsWith("-")) {
                k.append(ks.substring(1), "false");
            } else if (ks.startsWith("+")) {
                k.append(ks.substring(1), "true");
            } else {
                k.append(ks, "true");
            }
        }
        if (!jsReduce.trim().startsWith("function(")) {
            jsReduce = "function (obj,data) { " + jsReduce + " }";
        }
        if (jsFinalize == null) {
            jsFinalize = "";
        }
        if (!jsFinalize.trim().startsWith("function(")) {
            jsFinalize = "function (data) {" + jsFinalize + "}";
        }
        GroupCommand cmd = new GroupCommand(database.getCollection(config.getMapper().getCollectionName(q.getType())),
                k, q.toQueryObject(), ini, jsReduce, jsFinalize);
        return database.getCollection(config.getMapper().getCollectionName(q.getType())).group(cmd);
    }

    public <T> T findById(Class<T> type, ObjectId id) {
        T ret = getFromIDCache(type, id);
        if (ret != null) return ret;
        List<String> ls = config.getMapper().getFields(type, Id.class);
        if (ls.size() == 0) throw new RuntimeException("Cannot find by ID on non-Entity");

        return (T) createQueryFor(type).f(ls.get(0)).eq(id).get();
    }
//    /**
//     * returns a list of all elements for the given type, matching the given query
//     * @param qu - the query to search
//     * @param <T> - type of the elementyx
//     * @return  - list of elements matching query
//     */
//    public <T> List<T> readAll(Query<T> qu) {
//        inc(StatisticKeys.READS);
//        if (qu.getEntityClass().isAnnotationPresent(Cache.class)) {
//            if (isCached(qu.getEntityClass(), qu.toString())) {
//                inc(StatisticKeys.CHITS);
//                return getFromCache(qu.getEntityClass(), qu.toString());
//            } else {
//                inc(StatisticKeys.CMISS);
//            }
//        }
//        List<T> lst = qu.asList();
//        addToCache(qu.toString()+" / l:"+((QueryImpl)qu).getLimit()+" o:"+((QueryImpl)qu).getOffset(), qu.getEntityClass(), lst);
//        return lst;
//
//    }


    /**
     * does not set values in DB only in the entity
     *
     * @param toSetValueIn
     */
    public void setValueIn(Object toSetValueIn, String fld, Object value) {
        config.getMapper().setValue(toSetValueIn, value, fld);
    }

    public void setValueIn(Object toSetValueIn, Enum fld, Object value) {
        config.getMapper().setValue(toSetValueIn, value, fld.name());
    }

    public Object getValueOf(Object toGetValueFrom, String fld) {
        return config.getMapper().getValue(toGetValueFrom, fld);
    }

    public Object getValueOf(Object toGetValueFrom, Enum fld) {
        return config.getMapper().getValue(toGetValueFrom, fld.name());
    }


    @SuppressWarnings("unchecked")
    public <T> List<T> findByField(Class<T> cls, String fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
//        return createQueryFor(cls).field(fld).equal(val).asList();
    }

    public <T> List<T> findByField(Class<T> cls, Enum fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
//        return createQueryFor(cls).field(fld).equal(val).asList();
    }


    /**
     * deletes all objects matching the given query
     *
     * @param q
     * @param <T>
     */
    public <T> void delete(Query<T> q) {
        firePreRemoveEvent(q);
        WriteConcern wc = getWriteConcernForClass(q.getType());
        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(config.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject());
        } else {
            database.getCollection(config.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject(), wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(q.getType(), q.toQueryObject(), dur, false, WriteAccessType.BULK_DELETE);
        clearCacheIfNecessary(q.getType());
        firePostRemoveEvent(q);
    }


    /**
     * get a list of valid fields of a given record as they are in the MongoDB
     * so, if you have a field Mapping, the mapped Property-name will be used
     *
     * @param cls
     * @return
     */
    public final List<String> getFields(Class cls) {
        return config.getMapper().getFields(cls);
    }

    public final Class getTypeOfField(Class cls, String fld) {
        Field f = getField(cls, fld);
        if (f == null) return null;
        return f.getType();
    }

    public boolean storesLastChange(Class<? extends Object> cls) {
        return isAnnotationPresentInHierarchy(cls, StoreLastChange.class);
    }

    public boolean storesLastAccess(Class<? extends Object> cls) {
        return isAnnotationPresentInHierarchy(cls, StoreLastAccess.class);
    }

    public boolean storesCreation(Class<? extends Object> cls) {
        return isAnnotationPresentInHierarchy(cls, StoreCreationTime.class);
    }


    private String getFieldName(Class cls, String fld) {
        return config.getMapper().getFieldName(cls, fld);
    }

    /**
     * extended logic: Fld may be, the java field name, the name of the specified value in Property-Annotation or
     * the translated underscored lowercase name (mongoId => mongo_id)
     *
     * @param cls - class to search
     * @param fld - field name
     * @return field, if found, null else
     */
    private Field getField(Class cls, String fld) {
        return config.getMapper().getField(cls, fld);
    }

    public void setValue(Object in, String fld, Object val) {
        config.getMapper().setValue(in, val, fld);
    }

    public Object getValue(Object o, String fld) {
        return config.getMapper().getValue(o, fld);
    }

    public Long getLongValue(Object o, String fld) {
        return (Long) getValue(o, fld);
    }

    public String getStringValue(Object o, String fld) {
        return (String) getValue(o, fld);
    }

    public Date getDateValue(Object o, String fld) {
        return (Date) getValue(o, fld);
    }

    public Double getDoubleValue(Object o, String fld) {
        return (Double) getValue(o, fld);
    }


    /**
     * Erase cache entries for the given type. is being called after every store
     * depending on cache settings!
     *
     * @param cls
     */
    public void clearCachefor(Class<? extends Object> cls) {
        if (cache.get(cls) != null) {
            cache.get(cls).clear();
        }
        if (idCache.get(cls) != null) {
            idCache.get(cls).clear();
        }
        //clearCacheFor(cls);
    }

    public void storeInBackground(final Object lst) {
        inc(StatisticKeys.WRITES_CACHED);
        writers.execute(new Runnable() {
            @Override
            public void run() {
                storeNoCache(lst);
            }
        });
    }

    public void storeListInBackground(final List lst) {
        writers.execute(new Runnable() {
            @Override
            public void run() {
                storeNoCacheList(lst);
            }
        });
    }


    public ObjectId getId(Object o) {
        return config.getMapper().getId(o);
    }

    public void dropCollection(Class<? extends Object> cls) {
        if (!isAnnotationPresentInHierarchy(cls, NoProtection.class)) {
            try {
                if (accessDenied(cls.newInstance(), Permission.DROP)) {
                    throw new SecurityException("Drop of Collection denied!");
                }
            } catch (InstantiationException ex) {
                Logger.getLogger(Morphium.class.getName()).fatal(ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Morphium.class.getName()).fatal(ex);
            }
        }
        if (isAnnotationPresentInHierarchy(cls, Entity.class)) {
            firePreDropEvent(cls);
            long start = System.currentTimeMillis();
//            Entity entity = getAnnotationFromHierarchy(cls, Entity.class); //cls.getAnnotation(Entity.class);
            database.getCollection(config.getMapper().getCollectionName(cls)).drop();
            long dur = System.currentTimeMillis() - start;
            fireProfilingWriteEvent(cls, null, dur, false, WriteAccessType.DROP);
            firePostDropEvent(cls);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    public void ensureIndex(Class<?> cls, Map<String, Integer> index) {
        List<String> fields = getFields(cls);

        Map<String, Integer> idx = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> es : index.entrySet()) {
            String k = es.getKey();
            if (!fields.contains(k) && !fields.contains(config.getMapper().convertCamelCase(k))) {
                throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
            }
            String fn = config.getMapper().getFieldName(cls, k);
            idx.put(fn, es.getValue());
        }
        long start = System.currentTimeMillis();
        database.getCollection(config.getMapper().getCollectionName(cls)).ensureIndex(new BasicDBObject(idx));
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, new BasicDBObject(idx), dur, false, WriteAccessType.ENSURE_INDEX);
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * Similar to sorting
     *
     * @param cls
     * @param fldStr
     */
    public void ensureIndex(Class<?> cls, String... fldStr) {
        Map<String, Integer> m = new LinkedHashMap<String, Integer>();
        for (String f : fldStr) {
            int idx = 1;
            if (f.startsWith("-")) {
                idx = -1;
                f = f.substring(1);
            } else if (f.startsWith("+")) {
                f = f.substring(1);
            }
            m.put(f, idx);
        }
        ensureIndex(cls, m);
    }

    public void ensureIndex(Class<?> cls, Enum... fldStr) {
        Map<String, Integer> m = new LinkedHashMap<String, Integer>();
        for (Enum e : fldStr) {
            String f = e.name();
            m.put(f, 1);
        }
        ensureIndex(cls, m);
    }


    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public void store(Object o) {
        if (o instanceof List) {
            throw new RuntimeException("Lists need to be stored with storeList");
        }


        Class<?> type = getRealClass(o.getClass());
        if (!isAnnotationPresentInHierarchy(type, NoProtection.class)) { //o.getClass().isAnnotationPresent(NoProtection.class)) {
            if (getId(o) == null) {
                if (accessDenied(o, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(o, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }
        }

        Cache cc = getAnnotationFromHierarchy(type, Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || isAnnotationPresentInHierarchy(o.getClass(), NoCache.class)) {
            storeNoCache(o);
            return;
        }
        final Object fo = o;
        if (cc.writeCache()) {
            writers.execute(new Runnable() {
                @Override
                public void run() {
                    storeNoCache(fo);
                }
            });
            inc(StatisticKeys.WRITES_CACHED);

        } else {
            storeNoCache(o);
        }

    }

    public <T> void storeList(List<T> lst) {
        //have to sort list - might have different objects 
        List<T> storeDirect = new ArrayList<T>();
        List<T> storeInBg = new ArrayList<T>();

        //checking permission - might take some time ;-(
        for (T o : lst) {
            if (!isAnnotationPresentInHierarchy(o.getClass(), NoProtection.class)) {
                if (getId(o) == null) {
                    if (accessDenied(o, Permission.INSERT)) {
                        throw new SecurityException("Insert of new Object denied!");
                    }
                } else {
                    if (accessDenied(o, Permission.UPDATE)) {
                        throw new SecurityException("Update of Object denied!");
                    }
                }
            }

            Cache c = getAnnotationFromHierarchy(o.getClass(), Cache.class);//o.getClass().getAnnotation(Cache.class);
            if (c != null && !isAnnotationPresentInHierarchy(o.getClass(), NoCache.class)) {
                if (c.writeCache()) {
                    storeInBg.add(o);
                } else {
                    storeDirect.add(o);
                }
            } else {
                storeDirect.add(o);

            }
        }

        storeListInBackground(storeInBg);
        storeNoCacheList(storeDirect);

    }


    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o
     */
    public <T> void deleteObject(T o) {
        o = getRealObject(o);
        if (!isAnnotationPresentInHierarchy(o.getClass(), NoProtection.class)) {
            if (accessDenied(o, Permission.DELETE)) {
                throw new SecurityException("Deletion of Object denied!");
            }
        }
        firePreRemoveEvent(o);

        ObjectId id = config.getMapper().getId(o);
        BasicDBObject db = new BasicDBObject();
        db.append("_id", id);
        WriteConcern wc = getWriteConcernForClass(o.getClass());

        long start = System.currentTimeMillis();
        if (wc == null) {
            database.getCollection(config.getMapper().getCollectionName(o.getClass())).remove(db);
        } else {
            database.getCollection(config.getMapper().getCollectionName(o.getClass())).remove(db, wc);
        }
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
        clearCachefor(o.getClass());
        inc(StatisticKeys.WRITES);
        firePostRemoveEvent(o);
    }

    public void resetCache() {
        setCache(new Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>());
    }

    public void setCache(Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cache) {
        this.cache = cache;
    }


    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////
    //////////////////////////////
    /////////////// Statistics
    /////////
    /////
    ///
    public Map<String, Double> getStatistics() {
        return new Statistics(this);
    }

    public void removeEntryFromCache(Class cls, ObjectId id) {
        Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> c = cloneCache();
        Hashtable<Class<? extends Object>, Hashtable<ObjectId, Object>> idc = cloneIdCache();
        idc.get(cls).remove(id);

        ArrayList<String> toRemove = new ArrayList<String>();
        for (String key : c.get(cls).keySet()) {

            for (Object el : c.get(cls).get(key).getFound()) {
                ObjectId lid = config.getMapper().getId(el);
                if (lid == null) {
                    logger.error("Null id in CACHE?");
                    toRemove.add(key);
                }
                if (lid.equals(id)) {
                    toRemove.add(key);
                }
            }
        }
        for (String k : toRemove) {
            c.get(cls).remove(k);
        }
        setCache(c);
        setIdCache(idc);
    }

    public Map<StatisticKeys, StatisticValue> getStats() {
        return stats;
    }


    public void addShutdownListener(ShutdownListener l) {
        shutDownListeners.add(l);
    }

    public void removeShutdownListener(ShutdownListener l) {
        shutDownListeners.remove(l);
    }

    public void close() {
        cacheHousekeeper.end();

        for (ShutdownListener l : shutDownListeners) {
            l.onShutdown(this);
        }
        try {
            Thread.sleep(1000); //give it time to end ;-)
        } catch (Exception e) {
            logger.debug("Ignoring interrupted-exception");
        }
        if (cacheHousekeeper.isAlive()) {
            cacheHousekeeper.interrupt();
        }
        database = null;
        config = null;

        mongo.close();
        MorphiumSingleton.reset();
    }


    public String createCamelCase(String n) {
        return config.getMapper().createCamelCase(n, false);
    }

    /**
     * create a proxy object, implementing the ParitallyUpdateable Interface
     * these objects will be updated in mongo by only changing altered fields
     * <b>Attention:</b> the field name if determined by the setter name for now. That means, it does not honor the @Property-Annotation!!!
     * To make sure, you take the correct field - use the UpdatingField-Annotation for the setters!
     *
     * @param o
     * @param <T>
     * @return
     */
    public <T> T createPartiallyUpdateableEntity(T o) {
        return (T) Enhancer.create(o.getClass(), new Class[]{PartiallyUpdateable.class, Serializable.class}, new PartiallyUpdateableProxy(this, o));
    }

    public <T> T createLazyLoadedEntity(Class<T> cls, ObjectId id) {
        return (T) Enhancer.create(cls, new Class[]{Serializable.class}, new LazyDeReferencingProxy(this, cls, id));
    }

    protected <T> MongoField<T> createMongoField() {
        try {
            return (MongoField<T>) config.getFieldImplClass().newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public String getLastChangeField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreLastChange.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, LastChange.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }

    public String getLastChangeByField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreLastChange.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, LastChangeBy.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }

    public String getLastAccessField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreLastAccess.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, LastAccess.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }

    public String getLastAccessByField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreLastAccess.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, LastAccessBy.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }


    public String getCreationTimeField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreCreationTime.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, CreationTime.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }

    public String getCreatedByField(Class<?> cls) {
        if (!isAnnotationPresentInHierarchy(cls, StoreCreationTime.class)) return null;
        List<String> lst = config.getMapper().getFields(cls, CreatedBy.class);
        if (lst == null || lst.isEmpty()) return null;
        return lst.get(0);
    }


    //////////////////////////////////////////////////////
    ////////// SecuritySettings
    ///////
    /////
    ////
    ///
    //
    public MongoSecurityManager getSecurityManager() {
        return config.getSecurityMgr();
    }

    /**
     * temporarily switch off security settings - needed by SecurityManagers
     */
    public void setPrivileged() {
        privileged.add(Thread.currentThread());
    }

    public boolean checkAccess(String domain, Permission p) throws MongoSecurityException {
        if (privileged.contains(Thread.currentThread())) {
            privileged.remove(Thread.currentThread());
            return true;
        }
        return getSecurityManager().checkAccess(domain, p);
    }

    public boolean accessDenied(Class<?> cls, Permission p) throws MongoSecurityException {
        if (privileged.contains(Thread.currentThread())) {
            privileged.remove(Thread.currentThread());
            return false;
        }
        return !getSecurityManager().checkAccess(cls, p);
    }

    public boolean accessDenied(Object r, Permission p) throws MongoSecurityException {
        if (privileged.contains(Thread.currentThread())) {
            privileged.remove(Thread.currentThread());
            return false;
        }

        return !getSecurityManager().checkAccess(config.getMapper().getRealObject(r), p);
    }


}
