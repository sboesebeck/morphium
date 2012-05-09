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
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

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
public class Morphium {

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
    private ThreadPoolExecutor writers = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    //Cache by Type, query String -> CacheElement (contains list etc)
    private Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cache;
    private final Map<StatisticKeys, StatisticValue> stats;
    private Map<Class<?>, Map<Class<? extends Annotation>, Method>> lifeCycleMethods;
    /**
     * String Representing current user - needs to be set by Application
     */
    private String currentUser;
    private CacheHousekeeper cacheHousekeeper;

    private Vector<MorphiumStorageListener> listeners;
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
        config=cfg;
        privileged = new Vector<Thread>();
        shutDownListeners=new Vector<ShutdownListener>();
        listeners = new Vector<MorphiumStorageListener>();
        cache = new Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>();
        stats = new Hashtable<StatisticKeys, StatisticValue>();
        lifeCycleMethods = new Hashtable<Class<?>, Map<Class<? extends Annotation>, Method>>();
        for (StatisticKeys k : StatisticKeys.values()) {
            stats.put(k, new StatisticValue());
        }


        //dummyUser.setGroupIds();
        MongoOptions o = new MongoOptions();
        o.autoConnectRetry = true;
        o.fsync = true;
        o.connectTimeout = 2500;
        o.connectionsPerHost = config.getMaxConnections();
        o.socketKeepAlive = true;
        o.threadsAllowedToBlockForConnectionMultiplier = 5;
        o.safe = false;


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
        if (config.getMongoLogin()!=null) {
            if (!database.authenticate(config.getMongoLogin(),config.getMongoPassword().toCharArray())) {
                throw new RuntimeException("Authentication failed!");
            }
        }
        int cnt = database.getCollection("system.indexes").find().count(); //test connection

        if (config.getConfigManager()==null) {
            config.setConfigManager(new ConfigManager(this));
        }
        cacheHousekeeper = new CacheHousekeeper(this,5000, config.getGlobalCacheValidTime());
        cacheHousekeeper.start();


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
     * @param template
     * @param fields
     * @param <T>
     * @return
     */
    public <T> List<T> findByTemplate(T template, String... fields) {
        Class cls=template.getClass();
        List<String> flds=new ArrayList<String>();
        if (fields.length>0) {
            flds.addAll(Arrays.asList(fields));
        } else {
            flds=getFields(cls);
        }
        Query<T> q=createQueryFor(cls);
        for (String f:flds) {
            try {
                q.f(f).eq(getValue(template,f));
            } catch (IllegalAccessException e) {
                logger.error("Could not read field "+f+" of object "+cls.getName());
            }
        }
        return q.asList();
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    public void unset(Object toSet, String field) {
        if (toSet==null) throw new RuntimeException("Cannot update null!");
        if (getId(toSet)==null) {
            logger.info("just storing object as it is new...");
            store(toSet);
        }
        Class cls=toSet.getClass();
        String coll=config.getMapper().getCollectionName(cls);
        BasicDBObject query=new BasicDBObject();
        query.put("_id",getId(toSet));
        Field f=getField(cls, field);
        if (f==null) {
            throw new RuntimeException("Unknown field: "+field);
        }
        String fieldName=getFieldName(cls,field);

        BasicDBObject update=new BasicDBObject("$unset",new BasicDBObject(fieldName,1));
        database.getCollection(coll).update(query,update);

        clearCacheIfNecessary(cls);
        try {
            f.set(toSet,null);
        } catch (IllegalAccessException e) {
            //May happen, if null is not allowed for example
        }
    }


    private void clearCacheIfNecessary(Class cls) {
        if (cls.isAnnotationPresent(Cache.class)) {
            Cache c= (Cache) cls.getAnnotation(Cache.class);
            if (c.clearOnWrite()) {
                clearCachefor(cls);
            }
        }
    }

    private DBObject simplifyQueryObject(DBObject q) {
        if (q.keySet().size()==1 && q.get("$and")!=null) {
            BasicDBObject ret=new BasicDBObject();
            BasicDBList lst=(BasicDBList)q.get("$and");
            for (Object o:lst) {
                if (o instanceof DBObject) {
                    ret.putAll(((DBObject)o));
                } else if (o instanceof Map) {
                    ret.putAll(((Map)o));
                } else {
                    //something we cannot handle
                    return q;
                }
            }
            return ret;
        }
        return q;
    }

    public void set(Class<?> cls,Query<?> query, String field,Object val) {
        set(cls,query,field,val,false,false);
    }


    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     * @param cls - class or corresponding collection to take a look at
     * @param query - query to specify which objects should be set
     * @param values - map fieldName->Value, which values are to be set!
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be updated
     */
    public void set(Class<?> cls,Query<?> query, Map<String,Object> values, boolean insertIfNotExist, boolean multiple) {
        String coll=config.getMapper().getCollectionName(cls);
        BasicDBObject toSet=new BasicDBObject();
        for (String f:values.keySet()) {
            String fieldName=getFieldName(cls,f);
            toSet.put(fieldName,values.get(f));
        }
        DBObject qobj=query.toQueryObject();
        if (insertIfNotExist) {
            qobj=simplifyQueryObject(qobj);
        }
        BasicDBObject update=new BasicDBObject("$set",toSet);
        database.getCollection(coll).update(qobj,update,insertIfNotExist,multiple);
        clearCacheIfNecessary(cls);
    }
    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     * @param cls - class or corresponding collection to take a look at
     * @param query - query to specify which objects should be set
     * @param field - field to set
     * @param val - value to set
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple - update several documents, if false, only first hit will be updated
     */
    public void set(Class<?> cls,Query<?> query, String field,Object val, boolean insertIfNotExist, boolean multiple) {
        String coll=config.getMapper().getCollectionName(cls);
        String fieldName=getFieldName(cls,field);
        BasicDBObject update=new BasicDBObject("$set",new BasicDBObject(fieldName,val));
        DBObject qobj=query.toQueryObject();
        if (insertIfNotExist) {
            qobj=simplifyQueryObject(qobj);
        }
        database.getCollection(coll).update(qobj,update,insertIfNotExist,multiple);
        clearCacheIfNecessary(cls);
    }

    public void dec(Class<?> cls,Query<?> query, String field,int amount, boolean insertIfNotExist, boolean multiple) {
        inc(cls,query,field,-amount,insertIfNotExist,multiple);
    }
    public void dec(Class<?> cls,Query<?> query, String field,int amount) {
        inc(cls,query,field,-amount,false,false);
    }

    public void inc(Class<?> cls,Query<?> query, String field,int amount) {
        inc(cls,query,field,amount,false,false);
    }

    public void inc(Class<?> cls,Query<?> query, String field,int amount, boolean insertIfNotExist, boolean multiple) {
        String coll=config.getMapper().getCollectionName(cls);
        String fieldName=getFieldName(cls,field);
        BasicDBObject update=new BasicDBObject("$inc",new BasicDBObject(fieldName,amount));
        DBObject qobj=query.toQueryObject();
        if (insertIfNotExist) {
            qobj=simplifyQueryObject(qobj);
        }
        database.getCollection(coll).update(qobj,update,insertIfNotExist,multiple);
        clearCacheIfNecessary(cls);
    }



    /**
     * setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$set:{field:value}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: the field to change
     * @param value: the value to set
     */
    public void set(Object toSet, String field, Object value) {
        if (toSet==null) throw new RuntimeException("Cannot update null!");
        if (getId(toSet)==null) {
            logger.info("just storing object as it is new...");
            storeNoCache(toSet);
        }
        Class cls=toSet.getClass();
        String coll=config.getMapper().getCollectionName(cls);
        BasicDBObject query=new BasicDBObject();
        query.put("_id",getId(toSet));
        Field f=getField(cls, field);
        if (f==null) {
            throw new RuntimeException("Unknown field: "+field);
        }
        String fieldName=getFieldName(cls,field);

        BasicDBObject update=new BasicDBObject("$set",new BasicDBObject(fieldName,value));
        database.getCollection(coll).update(query,update);

        clearCacheIfNecessary(cls);
        try {
            f.set(toSet,value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * decreasing a value of a given object
     * calles <code>inc(toDec,field,-amount);</code>
     */
    public void dec(Object toDec, String field, int amount) {
        inc(toDec,field,-amount);
    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     * @param toInc: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: the field to change
     * @param amount: the value to set
     */
    public void inc(Object toInc, String field, int amount) {
        if (toInc==null) throw new RuntimeException("Cannot update null!");
        if (getId(toInc)==null) {
            logger.info("just storing object as it is new...");
            storeNoCache(toInc);
        }
        Class cls=toInc.getClass();
        String coll=config.getMapper().getCollectionName(cls);
        BasicDBObject query=new BasicDBObject();
        query.put("_id",getId(toInc));
        Field f=getField(cls, field);
        if (f==null) {
            throw new RuntimeException("Unknown field: "+field);
        }
        String fieldName=getFieldName(cls,field);

        BasicDBObject update=new BasicDBObject("$inc",new BasicDBObject(fieldName,amount));
        database.getCollection(coll).update(query,update);

        clearCacheIfNecessary(cls);

        //TODO: check inf necessary
        if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
            try {
                f.set(toInc,((Integer)f.get(toInc))+(int)amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Double.class)|| f.getType().equals(double.class)) {
            try {
                f.set(toInc,((Double)f.get(toInc))+amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Float.class)| f.getType().equals(float.class)) {
            try {
                f.set(toInc,((Float)f.get(toInc))+(float)amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
            try {
                f.set(toInc,((Long)f.get(toInc))+(long)amount);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.error("Could not set increased value - unsupported type "+cls.getName());
        }


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

    /**
     * create unique cache key for queries, also honoring skip & limit and sorting
     * @param q
     * @return
     */
    public String getCacheKey(Query q) {
        StringBuffer b = new StringBuffer();
        b.append(q.toQueryObject().toString());
        b.append(" l:");
        b.append(q.getLimit());
        b.append(" s:");
        b.append(q.getSkip());
        if (q.getOrder() != null) {
            b.append(" sort:");
            b.append(new BasicDBObject(q.getOrder()).toString());
        }
        return b.toString();
    }


    private void storeNoCacheUsingFields(Object ent, String... fields) {
        ObjectId id = getId(ent);
        if (ent==null) return;
        if (id==null) {
            //new object - update not working
            logger.warn("trying to partially update new object - storing it in full!");
            storeNoCache(ent);
            return;
        }
        Class<?> type=ent.getClass();
        firePreStoreEvent(ent);
        inc(StatisticKeys.WRITES);
        DBObject find=new BasicDBObject();

        find.put("_id", id);
        DBObject update=new BasicDBObject();
        for (String f:fields) {
            try {
                Object value = getValue(ent, f);
                if (value.getClass().isAnnotationPresent(Entity.class)) {
                    value=config.getMapper().marshall(value);
                }
                update.put(f, value);

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        if (type.isAnnotationPresent(StoreLastChange.class)) {
            StoreLastChange t = (StoreLastChange) type.getAnnotation(StoreLastChange.class);
            String ctf = t.lastChangeField();
            long now = System.currentTimeMillis();
            Field f = getField(type, ctf);
            if (f != null) {
                try {
                    f.set(ent, now);
                } catch (IllegalAccessException e) {
                    logger.error("Could not set modification time", e);

                }
            }
            update.put(ctf, now);
            if (t.storeLastChangeBy()) {
                ctf = t.lastChangeField();
                f = getField(type, ctf);
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


        update=new BasicDBObject("$set",update);
        database.getCollection(config.getMapper().getCollectionName(ent.getClass())).findAndModify(find,update);
        firePostStoreEvent(ent);
    }

    /**
     * updating an enty in DB without sending the whole entity
     * only transfers the fields to be changed / set
     * @param ent
     * @param fields
     */
    public void updateUsingFields(final Object ent,final String... fields) {
        if (ent==null) return;
        if (fields.length==0) return; //not doing an update - no change
        if (!ent.getClass().isAnnotationPresent(NoProtection.class)) {
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

        if (ent.getClass().isAnnotationPresent(NoCache.class)) {
            storeNoCacheUsingFields(ent, fields);
            return;
        }

        Cache cc = ent.getClass().getAnnotation(Cache.class);
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


    public void callLifecycleMethod(Class<? extends Annotation> type, Object on) {
        if (on == null) return;
        //No synchronized block - might cause the methods to be put twice into the
        //hashtabel - but for performance reasons, it's ok...
        Class<?> cls = on.getClass();
        //No Lifecycle annotation - no method calling
        if (!cls.isAnnotationPresent(Lifecycle.class)) {
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

    private void firePreStoreEvent(Object o) {
        if (o == null) return;
        for (MorphiumStorageListener l : listeners) {
            l.preStore(o);
        }
        callLifecycleMethod(PreStore.class, o);

    }

    private void firePostStoreEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postStore(o);
        }
        callLifecycleMethod(PostStore.class, o);
        //existing object  => store last Access, if needed

    }

    private void firePreDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.preDrop(cls);
        }

    }

    private void firePostDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.postDrop(cls);
        }
    }

    private void firePostRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(o);
        }
        callLifecycleMethod(PostRemove.class, o);
    }

    private void firePostRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(q);
        }
        //TODO: FIX - Cannot call lifecycle method here

    }

    private void firePreRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.preDelete(o);
        }
        callLifecycleMethod(PreRemove.class, o);
    }

    private void firePreRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.preRemove(q);
        }
        //TODO: Fix - cannot call lifecycle method
    }

    private void firePreListStoreEvent(List lst) {
        for (MorphiumStorageListener l : listeners) {
            l.preListStore(lst);
        }
        for (Object o : lst) {
            callLifecycleMethod(PreStore.class, o);
        }
    }

    private void firePostListStoreEvent(List lst) {
        for (MorphiumStorageListener l : listeners) {
            l.postListStore(lst);
        }
        for (Object o : lst) {
            callLifecycleMethod(PostStore.class, o);
        }

    }

    /**
     * will be called by query after unmarshalling
     *
     * @param o
     */
    protected void firePostLoadEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postLoad(o);
        }
        callLifecycleMethod(PostLoad.class, o);

    }

    private void storeNoCache(Object o) {
        Class type = o.getClass();
        if (!type.isAnnotationPresent(Entity.class)) {
            throw new RuntimeException("Not an entity! Storing not possible!");
        }

        ObjectId id = config.getMapper().getId(o);
        if (o instanceof PartiallyUpdateable && id!=null) {
            updateUsingFields(o, ((PartiallyUpdateable) o).getAlteredFields().toArray(new String[((PartiallyUpdateable) o).getAlteredFields().size()]));
            return;
        }
        inc(StatisticKeys.WRITES);
        firePreStoreEvent(o);

        DBObject marshall = config.getMapper().marshall(o);
        boolean isNew=id==null;

        if (isNew) {

            //new object - need to store creation time
            if (type.isAnnotationPresent(StoreCreationTime.class)) {
                StoreCreationTime t = (StoreCreationTime) type.getAnnotation(StoreCreationTime.class);
                String ctf = t.creationTimeField();
                long now = System.currentTimeMillis();
                Field f = getField(type, ctf);
                if (f != null) {
                    try {
                        f.set(o, now);
                    } catch (IllegalAccessException e) {
                        logger.error("Could not set creation time", e);

                    }
                }
                marshall.put(ctf, now);
                if (t.storeCreatedBy()) {
                    ctf = t.createdByField();
                    f = getField(type, ctf);
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
        if (type.isAnnotationPresent(StoreLastChange.class)) {
            StoreLastChange t = (StoreLastChange) type.getAnnotation(StoreLastChange.class);
            String ctf = t.lastChangeField();
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
            if (t.storeLastChangeBy()) {
                ctf = t.lastChangeField();
                f = getField(type, ctf);
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


        database.getCollection(config.getMapper().getCollectionName(o.getClass())).save(marshall);
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

        if (o.getClass().isAnnotationPresent(Cache.class)) {
            if (o.getClass().getAnnotation(Cache.class).clearOnWrite()) {
                clearCachefor(o.getClass());
            }
        }

        firePostStoreEvent(o);
    }

    private void storeNoCacheList(List o) {

        if (!o.isEmpty()) {
            List<Class> clearCaches = new ArrayList<Class>();
            firePreListStoreEvent(o);
            for (Object c : o) {
                storeNoCache(c);
            }
            firePostListStoreEvent(o);
        }
    }


    protected boolean isCached(Class<? extends Object> type, String k) {
        if (type.isAnnotationPresent(Cache.class)) {
            Cache c = type.getAnnotation(Cache.class);
            if (!c.readCache()) return false;
        } else {
            return false;
        }
        return cache.get(type) != null && cache.get(type).get(k) != null && cache.get(type).get(k).getFound() != null;
    }

    public <T> List<T> getFromCache(Class<T> type, String k) {
        if (cache.get(type) == null || cache.get(type).get(k) == null) return null;
        final CacheElement cacheElement = cache.get(type).get(k);
        cacheElement.setLru(System.currentTimeMillis());
        return cacheElement.getFound();
    }

    public Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cloneCache() {
        return (Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>>) cache.clone();
    }

    public void clearCollection(Class<? extends Object> cls) {
        if (!cls.isAnnotationPresent(NoProtection.class)) {
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

        clearCachefor(cls);


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
        return new QueryImpl<T>(this,type, config.getMapper());
    }

    public <T> List<T> find(Query<T> q) {
        return q.asList();
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



    @SuppressWarnings("unchecked")
    public <T> List<T> findByField(Class<T> cls, String fld, Object val) {
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
        database.getCollection(config.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject());
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
        return cls.isAnnotationPresent(StoreLastChange.class);
    }

    public String getLastChangeField(Class<? extends Object> cls) {
        if (storesLastChange(cls)) {
            StoreLastChange slc = cls.getAnnotation(StoreLastChange.class);
            return slc.lastChangeField();
        }
        return null;
    }

    public String getLastChangeByField(Class<? extends Object> cls) {
        if (storesLastChange(cls)) {
            StoreLastChange slc = cls.getAnnotation(StoreLastChange.class);
            return slc.storeLastChangeBy() ? slc.lastChangeByField() : null;
        }
        return null;
    }

    private String getFieldName(Class cls,String fld) {
        return config.getMapper().getFieldName(cls,fld);
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

    public void setValue(Object in, String fld, Object val) throws IllegalAccessException {
        Field f = getField(in.getClass(), fld);
        if (f == null) {
            throw new IllegalAccessException("Field " + fld + " not found");
        }
        f.set(in, val);
    }

    public Object getValue(Object o, String fld) throws IllegalAccessException {
        Field f = getField(o.getClass(), fld);
        if (f == null) {
            throw new IllegalAccessException("Field " + fld + " not found");
        }
        return f.get(o);
    }

    public Long getLongValue(Object o, String fld) throws IllegalAccessException {
        return (Long) getValue(o, fld);
    }

    public String getStringValue(Object o, String fld) throws IllegalAccessException {
        return (String) getValue(o, fld);
    }

    public Date getDateValue(Object o, String fld) throws IllegalAccessException {
        return (Date) getValue(o, fld);
    }

    public Double getDoubleValue(Object o, String fld) throws IllegalAccessException {
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
        //clearCacheFor(cls);
    }

    public void storeInBackground(final Object lst) {
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
        if (!o.getClass().isAnnotationPresent(Entity.class)) {
            throw new RuntimeException("No Entitiy");
        }
        for (Field f : o.getClass().getDeclaredFields()) {
            if (f.isAnnotationPresent(Id.class)) {
                try {
                    f.setAccessible(true);
                    return (ObjectId) f.get(o);
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(Morphium.class.getName()).error(ex);
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(Morphium.class.getName()).error(ex);
                }
            }
        }
        throw new RuntimeException("No ID-Field found");
    }

    public void dropCollection(Class<? extends Object> cls) {
        if (!cls.isAnnotationPresent(NoProtection.class)) {
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
        if (cls.isAnnotationPresent(Entity.class)) {
            firePreDropEvent(cls);
            Entity entity = cls.getAnnotation(Entity.class);
            database.getCollection(config.getMapper().getCollectionName(cls)).drop();
            firePostDropEvent(cls);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    public void ensureIndex(Class<?> cls, Map<String, Integer> index) {
        List<String> fields = getFields(cls);
        for (String k : index.keySet()) {
            if (!fields.contains(k)) {
                throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
            }
        }
        database.getCollection(config.getMapper().getCollectionName(cls)).ensureIndex(new BasicDBObject(index));
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * Similar to sorting
     *
     * @param cls
     * @param fldStr
     */
    public void ensureIndex(Class<?> cls, String... fldStr) {
        Map<String, Integer> m = new HashMap<String, Integer>();
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


    /**
     * Stores a single Object. Clears the corresponding cache
     *
     * @param o - Object to store
     */
    public void store(final Object o) {
        if (o instanceof List) {
            throw new RuntimeException("Lists need to be stored with storeList");
        }


        if (!o.getClass().isAnnotationPresent(NoProtection.class)) {
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

        if (o.getClass().isAnnotationPresent(NoCache.class)) {
            storeNoCache(o);
            return;
        }

        Cache cc = o.getClass().getAnnotation(Cache.class);
        if (cc != null) {
            if (cc.writeCache()) {
                writers.execute(new Runnable() {
                    @Override
                    public void run() {
                        storeNoCache(o);
                    }
                });
                inc(StatisticKeys.WRITES_CACHED);

            } else {
                storeNoCache(o);
            }
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
            if (!o.getClass().isAnnotationPresent(NoProtection.class)) {
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
            if (o.getClass().getAnnotation(NoCache.class) != null) {
                storeDirect.add(o);
            } else {
                storeInBg.add(o);
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
        if (!o.getClass().isAnnotationPresent(NoProtection.class)) {
            if (accessDenied(o, Permission.DELETE)) {
                throw new SecurityException("Deletion of Object denied!");
            }
        }
        firePreRemoveEvent(o);

        ObjectId id = config.getMapper().getId(o);
        BasicDBObject db = new BasicDBObject();
        db.append("_id", id);
        database.getCollection(config.getMapper().getCollectionName(o.getClass())).remove(db);

        clearCachefor(o.getClass());
        inc(StatisticKeys.WRITES);
        firePostRemoveEvent(o);
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
        return new StatisticsMap();
    }

    public enum StatisticKeys {

        WRITES, WRITES_CACHED, READS, CHITS, CMISS, NO_CACHED_READS, CHITSPERC, CMISSPERC, CACHE_ENTRIES, WRITE_BUFFER_ENTRIES
    }

    public class StatisticValue {

        private long value = 0;

        public void inc() {
            value++;
        }

        public void dec() {
            value--;
        }

        public long get() {
            return value;
        }
    }

    private class StatisticsMap extends Hashtable<String, Double> {

        /**
         *
         */
        private static final long serialVersionUID = -2831335094438480701L;

        @SuppressWarnings("rawtypes")
        public StatisticsMap() {
            for (StatisticKeys k : stats.keySet()) {
                super.put(k.name(), (double) stats.get(k).get());
            }
            double entries = 0;
            for (Class k : cache.keySet()) {
                entries += cache.get(k).size();
                super.put("X-Entries for: " + k.getName(), (double) cache.get(k).size());
            }
            super.put(StatisticKeys.CACHE_ENTRIES.name(), entries);

            entries = 0;

            super.put(StatisticKeys.WRITE_BUFFER_ENTRIES.name(), Double.valueOf((double) writeBufferCount()));
            super.put(StatisticKeys.CHITSPERC.name(), ((double) stats.get(StatisticKeys.CHITS).get()) / (stats.get(StatisticKeys.READS).get() - stats.get(StatisticKeys.NO_CACHED_READS).get()) * 100.0);
            super.put(StatisticKeys.CMISSPERC.name(), ((double) stats.get(StatisticKeys.CMISS).get()) / (stats.get(StatisticKeys.READS).get() - stats.get(StatisticKeys.NO_CACHED_READS).get()) * 100.0);
        }

        @Override
        public synchronized Double put(String arg0, Double arg1) {
            throw new RuntimeException("not allowed!");
        }

        @Override
        public synchronized void putAll(@SuppressWarnings("rawtypes") Map arg0) {
            throw new RuntimeException("not allowed");
        }

        @Override
        public synchronized Double remove(Object arg0) {
            throw new RuntimeException("not allowed");
        }

        @Override
        public String toString() {
            StringBuffer b = new StringBuffer();
            String[] lst = keySet().toArray(new String[keySet().size()]);
            Arrays.sort(lst);
            for (String k : lst) {
                b.append("- ");
                b.append(k);
                b.append("\t");
                b.append(get(k));
                b.append("\n");
            }
            return b.toString();
        }
    }

    public void addShutdownListener(ShutdownListener l) {
        shutDownListeners.add(l);
    }
    public void removeShutdownListener(ShutdownListener l) {
        shutDownListeners.remove(l);
    }
    public void close() {
        cacheHousekeeper.end();

        for (ShutdownListener l:shutDownListeners) {
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
       return  (T) Enhancer.create(o.getClass(),new Class[]{PartiallyUpdateable.class},new PartiallyUpdateableInvocationHandler());
    }

    protected <T> MongoField<T> createMongoField() {
        try {
            return (MongoField<T>) Class.forName(config.getFieldImplClass()).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * CGLib Interceptor to create a transparent Proxy for partially updateable Entities
     */
    private class PartiallyUpdateableInvocationHandler implements MethodInterceptor,PartiallyUpdateable {
        private List<String> updateableFields;

        public PartiallyUpdateableInvocationHandler() {
            updateableFields=new Vector<String>();
        }


        @Override
        public List<String> getAlteredFields() {
            return updateableFields;
        }

        @Override
        public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
            if (method.getName().startsWith("set")) {
                if (method.isAnnotationPresent(UpdatingField.class)) {
                    UpdatingField up=method.getAnnotation(UpdatingField.class);
                    if (!getFields(o.getClass()).contains(up.value())) {
                        throw new IllegalArgumentException("Field "+up.value()+" is not known to Type "+o.getClass().getName());
                    }
                    updateableFields.add(up.value());
                } else {
                    String n = method.getName().substring(3);
                    n = n.substring(0, 1).toLowerCase() + n.substring(1);
                    updateableFields.add(n);
                }
            }
            if (method.getName().equals("getAlteredFields")) {
                return getAlteredFields();
            }
            return methodProxy.invokeSuper(o, objects);
        }
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
        return !getSecurityManager().checkAccess(r, p);
    }


}
