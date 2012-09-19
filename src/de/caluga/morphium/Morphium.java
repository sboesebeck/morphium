/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import com.mongodb.*;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.annotations.security.NoProtection;
import de.caluga.morphium.cache.CacheElement;
import de.caluga.morphium.cache.CacheHousekeeper;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.ConfNode;
import de.caluga.morphium.replicaset.ReplicaSetConf;
import de.caluga.morphium.replicaset.ReplicaSetNode;
import de.caluga.morphium.secure.MongoSecurityException;
import de.caluga.morphium.secure.MongoSecurityManager;
import de.caluga.morphium.secure.Permission;
import de.caluga.morphium.validation.JavaxValidationStorageListener;
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

    private List<MorphiumStorageListener> listeners;
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
        listeners = new ArrayList<MorphiumStorageListener>();
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
        if (config.isTimeoutBugWorkAroundEnabled()) {
            mongo.setReadPreference(ReadPreference.primary());
        } else {
            if (config.getDefaultReadPreference() != null) {
                mongo.setReadPreference(config.getDefaultReadPreference().getPref());
            }
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
        if (config.getWriter() == null) {
            config.setWriter(new WriterImpl());
        }
        config.getWriter().setMorphium(this);

        // enable/disable javax.validation support
        if (hasValidationSupport()) {
            logger.info("Adding javax.validation Support...");
            addListener(new JavaxValidationStorageListener());
        }

        logger.info("Initialization successful...");

    }

    /**
     * Checks if javax.validation is available and enables validation support.
     *
     * @return
     */
    private boolean hasValidationSupport() {
        try {
            Class c = getClass().getClassLoader().loadClass("javax.validation.ValidatorFactory");
        } catch (ClassNotFoundException cnf) {
            return false;
        }
        return true;
    }

    public void addListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<MorphiumStorageListener>();
        newList.addAll(listeners);
        newList.add(lst);
        listeners = newList;
    }

    public void removeListener(MorphiumStorageListener lst) {
        List<MorphiumStorageListener> newList = new ArrayList<MorphiumStorageListener>();
        newList.addAll(listeners);
        newList.remove(lst);
        listeners = newList;
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

    public void unset(final Object toSet, final String field) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");

        if (!isAnnotationPresentInHierarchy(toSet.getClass(), NoProtection.class)) {
            if (accessDenied(toSet.getClass(), Permission.UPDATE)) {
                throw new SecurityException("Update of Object denied!");
            }
        }
        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
        Cache c = getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().unset(toSet, field);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().unset(toSet, field);
                firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
            }
        });
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


    public void clearCacheIfNecessary(Class cls) {
        Cache c = getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
        if (c != null) {
            if (c.clearOnWrite()) {
                clearCachefor(cls);
            }
        }
    }

    public DBObject simplifyQueryObject(DBObject q) {
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

    public void push(final Query<?> query, final Enum field, final Object value) {
        push(query, field, value, false, true);
    }

    public void pull(Query<?> query, Enum field, Object value) {
        pull(query, field.name(), value, false, true);
    }

    public void push(Query<?> query, String field, Object value) {
        push(query, field, value, false, true);
    }

    public void pull(Query<?> query, String field, Object value) {
        pull(query, field, value, false, true);
    }


    public void push(Query<?> query, Enum field, Object value, boolean insertIfNotExist, boolean multiple) {
        push(query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pull(Query<?> query, Enum field, Object value, boolean insertIfNotExist, boolean multiple) {
        pull(query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pushAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        push(query, field.name(), value, insertIfNotExist, multiple);
    }

    public void pullAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pull(query, field.name(), value, insertIfNotExist, multiple);
    }


    public void push(final Query<?> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        if (accessDenied(query.getType(), Permission.UPDATE)) {
            throw new SecurityException("Update of Object denied!");
        }
        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
        if (!isWriteCached(query.getType())) {
            config.getWriter().pushPull(true, query, field, value, insertIfNotExist, multiple);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPull(true, query, field, value, insertIfNotExist, multiple);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            }
        });
    }

    public void pull(final Query<?> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        if (accessDenied(query.getType(), Permission.UPDATE)) {
            throw new SecurityException("Update of Object denied!");
        }
        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
        if (!isWriteCached(query.getType())) {
            config.getWriter().pushPull(false, query, field, value, insertIfNotExist, multiple);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPull(false, query, field, value, insertIfNotExist, multiple);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
            }
        });
    }

    public void pushAll(final Query<?> query, final String field, final List<Object> value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        if (accessDenied(query.getType(), Permission.UPDATE)) {
            throw new SecurityException("Update of Object denied!");
        }
        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
        if (!isWriteCached(query.getType())) {
            config.getWriter().pushPullAll(true, query, field, value, insertIfNotExist, multiple);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPullAll(true, query, field, value, insertIfNotExist, multiple);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            }
        });

    }

    public void pullAll(Query<?> query, String field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pull(query, field, value, insertIfNotExist, multiple);
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

    public void set(final Query<?> query, final Map<String, Object> map, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null) throw new RuntimeException("Cannot update null!");

        if (accessDenied(query.getType(), Permission.UPDATE)) {
            throw new SecurityException("Update of Object denied!");
        }
        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = getAnnotationFromHierarchy(query.getType(), Cache.class);
        if (isAnnotationPresentInHierarchy(query.getType(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().set(query, map, insertIfNotExist, multiple);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(query, map, insertIfNotExist, multiple);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
            }
        });
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

    public void inc(final Query<?> query, final String name, final int amount, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null) throw new RuntimeException("Cannot update null!");

        if (!isAnnotationPresentInHierarchy(query.getType(), NoProtection.class)) {
            if (accessDenied(query.getType(), Permission.UPDATE)) {
                throw new SecurityException("Update of Object denied!");
            }
        }
        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
        Cache c = getAnnotationFromHierarchy(query.getType(), Cache.class);
        if (isAnnotationPresentInHierarchy(query.getType(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().inc(query, name, amount, insertIfNotExist, multiple);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().inc(query, name, amount, insertIfNotExist, multiple);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            }
        });
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
    public void set(final Object toSet, final String field, final Object value) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");

        if (!isAnnotationPresentInHierarchy(toSet.getClass(), NoProtection.class)) {
            if (getId(toSet) == null) {
                if (accessDenied(toSet, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(toSet, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }
        }
        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().set(toSet, field, value);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(toSet, field, value);
                firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            }
        });
    }

    /**
     * decreasing a value of a given object
     * calles <code>inc(toDec,field,-amount);</code>
     */
    public void dec(Object toDec, String field, int amount) {
        inc(toDec, field, -amount);
    }

    public void inc(final Object toSet, final String field, final int i) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");

        if (!isAnnotationPresentInHierarchy(toSet.getClass(), NoProtection.class)) {
            if (getId(toSet) == null) {
                if (accessDenied(toSet, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(toSet, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }
        }
        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
        Cache c = getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().inc(toSet, field, i);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(toSet, field, i);
                firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            }
        });
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

    public void setPrivilegedThread(Thread thr) {

    }


    public void inc(StatisticKeys k) {
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
            config.getWriter().storeUsingFields(ent, fields);
            return;
        }

        firePreUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = getAnnotationFromHierarchy(ent.getClass(), Cache.class);
        if (isAnnotationPresentInHierarchy(ent.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().storeUsingFields(ent, fields);
            firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().storeUsingFields(ent, fields);
                firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            }
        });
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

    public ObjectMapper getMapper() {
        return config.getMapper();
    }

    Class<?> getRealClass(Class<?> cls) {
        return config.getMapper().getRealClass(cls);
    }

    <T> T getRealObject(T o) {
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
                if (java.lang.reflect.Modifier.isStatic(fld.getModifiers())) {
                    continue;
                }
                try {
                    fld.set(o, fld.get(fromDb));
                } catch (IllegalAccessException e) {
                    logger.error("Could not set Value: " + fld);
                }
            }
            firePostLoadEvent(o);
        } else {
            logger.info("Did not find object with id " + id);
            return null;
        }
        return o;
    }

    public void firePreStoreEvent(Object o, boolean isNew) {
        if (o == null) return;
        for (MorphiumStorageListener l : listeners) {
            l.preStore(o, isNew);
        }
        callLifecycleMethod(PreStore.class, o);

    }

    public void firePostStoreEvent(Object o, boolean isNew) {
        for (MorphiumStorageListener l : listeners) {
            l.postStore(o, isNew);
        }
        callLifecycleMethod(PostStore.class, o);
        //existing object  => store last Access, if needed

    }

    public void firePreDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.preDrop(cls);
        }

    }

    public void firePostDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.postDrop(cls);
        }
    }

    public void firePostUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.postUpdate(cls, t);
        }
    }

    public void firePreUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.preUpdate(cls, t);
        }
    }

    public void firePostRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(o);
        }
        callLifecycleMethod(PostRemove.class, o);
    }

    public void firePostRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(q);
        }
        //TODO: FIX - Cannot call lifecycle method here
    }

    public void firePreRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.preDelete(o);
        }
        callLifecycleMethod(PreRemove.class, o);
    }

    public void firePreRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.preRemove(q);
        }
        //TODO: Fix - cannot call lifecycle method
    }

    /**
     * will be called by query after unmarshalling
     *
     * @param o
     */
    public void firePostLoadEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postLoad(o);
        }
        callLifecycleMethod(PostLoad.class, o);
    }


    /**
     * same as retReplicaSetStatus(false);
     *
     * @return
     */
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus() {
        return getReplicaSetStatus(false);
    }

    /**
     * get the current replicaset status - issues the replSetGetStatus command to mongo
     * if full==true, also the configuration is read. This method is called with full==false for every write in
     * case a Replicaset is configured to find out the current number of active nodes
     *
     * @param full
     * @return
     */
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus(boolean full) {
        if (config.getMode().equals(MongoDbMode.REPLICASET)) {
            try {
                CommandResult res = getMongo().getDB("admin").command("replSetGetStatus");
                de.caluga.morphium.replicaset.ReplicaSetStatus status = getConfig().getMapper().unmarshall(de.caluga.morphium.replicaset.ReplicaSetStatus.class, res);
                if (full) {
                    DBCursor rpl = getMongo().getDB("local").getCollection("system.replset").find();
                    DBObject stat = rpl.next(); //should only be one, i think
                    ReplicaSetConf cfg = getConfig().getMapper().unmarshall(ReplicaSetConf.class, stat);
                    List<Object> mem = cfg.getMemberList();
                    List<ConfNode> cmembers = new ArrayList<ConfNode>();

                    for (Object o : mem) {
                        DBObject dbo = (DBObject) o;
                        ConfNode cn = getConfig().getMapper().unmarshall(ConfNode.class, dbo);
                        cmembers.add(cn);
                    }
                    cfg.setMembers(cmembers);
                    status.setConfig(cfg);
                }
                //de-referencing list
                List lst = status.getMembers();
                List<ReplicaSetNode> members = new ArrayList<ReplicaSetNode>();
                for (Object l : lst) {
                    DBObject o = (DBObject) l;
                    ReplicaSetNode n = getConfig().getMapper().unmarshall(ReplicaSetNode.class, o);
                    members.add(n);
                }
                status.setMembers(members);

                return status;
            } catch (Exception e) {
                logger.error("Could not get Replicaset status", e);
            }
        }
        return null;
    }

    public boolean isReplicaSet() {
        return config.getMode().equals(MongoDbMode.REPLICASET);
    }

    public WriteConcern getWriteConcernForClass(Class<?> cls) {
        if (logger.isDebugEnabled()) logger.debug("returning write concern for " + cls.getSimpleName());
        WriteSafety safety = getAnnotationFromHierarchy(cls, WriteSafety.class);  // cls.getAnnotation(WriteSafety.class);
        if (safety == null) return null;
        boolean fsync = safety.waitForSync();
        boolean j = safety.waitForJournalCommit();

        if (j && fsync) {
            fsync = false;
        }
        int w = safety.level().getValue();
        if (!isReplicaSet() && w > 1 || config.isTimeoutBugWorkAroundEnabled()) {
            w = 1;
        }
        int timeout = safety.timeout();
        if (isReplicaSet() && w > 2) {
            de.caluga.morphium.replicaset.ReplicaSetStatus s = getReplicaSetStatus();
            if (s == null || s.getActiveNodes() == 0) {
                logger.warn("ReplicaSet status is null or no node active! Assuming default write concern");
                return null;
            }
            if (logger.isDebugEnabled()) logger.debug("Active nodes now: " + s.getActiveNodes());
            int activeNodes = s.getActiveNodes();
            if (timeout == 0) {
                if (getConfig().getConnectionTimeout() == 0) {
                    if (logger.isDebugEnabled())
                        logger.debug("Not waiting for all slaves withoug timeout - unfortunately no connection timeout set in config - setting to 10s, Type: " + cls.getSimpleName());
                    timeout = 10000;
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug("Not waiting for all slaves without timeout - could cause deadlock. Setting to connectionTimeout value, Type: " + cls.getSimpleName());
                    timeout = getConfig().getConnectionTimeout();
                }
            }
            //Wait for all active slaves
            w = activeNodes;
        }
//        if (w==0) {
//            return WriteConcern.NONE;
//        }
//        if(w==1) {
//            return WriteConcern.FSYNC_SAFE;
//        }
//        if (w==2) {
//            return WriteConcern.JOURNAL_SAFE;
//        }
//        if (w==3) {
//            return WriteConcern.REPLICAS_SAFE;
//        }
        if (w == -99) {
            return new WriteConcern("majority", timeout, fsync, j);
        }
        return new WriteConcern(w, timeout, fsync, j);
    }

    public void addProfilingListener(ProfilingListener l) {
        profilingListeners.add(l);
    }

    public void removeProfilingListener(ProfilingListener l) {
        profilingListeners.remove(l);
    }

    public void fireProfilingWriteEvent(Class type, Object data, long time, boolean isNew, WriteAccessType wt) {
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


    public boolean isCached(Class<? extends Object> type, String k) {
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
        if (!isAnnotationPresentInHierarchy(cls, NoProtection.class)) { //cls.isAnnotationPresent(NoProtection.class)) {
            try {
                if (accessDenied(cls.newInstance(), Permission.DROP)) {
                    throw new SecurityException("Drop / clear of Collection denied!");
                }
            } catch (InstantiationException ex) {
                Logger.getLogger(Morphium.class).error(ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Morphium.class).error(ex);
            }
        }
        firePreDropEvent(cls);
        delete(createQueryFor(cls));
        firePostDropEvent(cls);
    }

    /**
     * clears every single object in collection - reads ALL objects to do so
     * this way Lifecycle methods can be called!
     *
     * @param cls
     */

    public void clearCollectionOneByOne(Class<? extends Object> cls) {
        if (!isAnnotationPresentInHierarchy(cls, NoProtection.class)) { //cls.isAnnotationPresent(NoProtection.class)) {
            try {
                if (accessDenied(cls.newInstance(), Permission.DROP)) {
                    throw new SecurityException("Drop / clear of Collection denied!");
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
            delete(r);
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
        Query<T> q = config.getQueryFact().createQuery(this, type);
        q.setMorphium(this);
        return q;
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
        DBCollection collection = database.getCollection(config.getMapper().getCollectionName(cls));
        setReadPreference(collection, cls);
        return collection.distinct(key, new BasicDBObject());
    }

    private void setReadPreference(DBCollection c, Class type) {
        DefaultReadPreference pr = getAnnotationFromHierarchy(type, DefaultReadPreference.class);
        if (pr != null) {
            c.setReadPreference(pr.value().getPref());
        } else {
            c.setReadPreference(null);
        }
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


    public String getFieldName(Class cls, String fld) {
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
    public Field getField(Class cls, String fld) {
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

    public void storeNoCache(Object lst) {
        config.getWriter().store(lst);
    }

    public void storeInBackground(final Object lst) {
        inc(StatisticKeys.WRITES_CACHED);
        writers.execute(new Runnable() {
            @Override
            public void run() {
                boolean isNew = getId(lst) == null;
                firePreStoreEvent(lst, isNew);
                config.getWriter().store(lst);
                firePostStoreEvent(lst, isNew);
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

        if (config.getMode() == MongoDbMode.REPLICASET && config.getAdr().size() > 1) {
            //replicaset
            logger.warn("Cannot drop collection for class " + cls.getSimpleName() + " as we're in a clustered environment (Driver 2.8.0)");
            clearCollection(cls);
            return;
        }

        if (isAnnotationPresentInHierarchy(cls, Entity.class)) {
            firePreDropEvent(cls);
            long start = System.currentTimeMillis();
//            Entity entity = getAnnotationFromHierarchy(cls, Entity.class); //cls.getAnnotation(Entity.class);

            DBCollection coll = database.getCollection(config.getMapper().getCollectionName(cls));
//            coll.setReadPreference(com.mongodb.ReadPreference.PRIMARY);
            coll.drop();
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
        final boolean isNew = getId(o) == null;
        if (!isAnnotationPresentInHierarchy(type, NoProtection.class)) { //o.getClass().isAnnotationPresent(NoProtection.class)) {
            if (isNew) {
                if (accessDenied(o, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(o, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }
        }
        firePreStoreEvent(o, isNew);
        Cache cc = getAnnotationFromHierarchy(type, Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().store(o);
            firePostStoreEvent(o, isNew);
            return;
        }
        final Object fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().store(fo);
                firePostStoreEvent(fo, isNew);
            }
        });
        inc(StatisticKeys.WRITES_CACHED);

    }


    public <T> void storeList(List<T> lst) {
        //have to sort list - might have different objects 
        List<T> storeDirect = new ArrayList<T>();
        final List<T> storeInBg = new ArrayList<T>();

        //checking permission - might take some time ;-(
        for (T o : lst) {
            if (getId(o) == null) {
                if (accessDenied(o, Permission.INSERT)) {
                    throw new SecurityException("Insert of new Object denied!");
                }
            } else {
                if (accessDenied(o, Permission.UPDATE)) {
                    throw new SecurityException("Update of Object denied!");
                }
            }

            Cache c = getAnnotationFromHierarchy(o.getClass(), Cache.class);//o.getClass().getAnnotation(Cache.class);
            if (isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || c == null || !c.writeCache()) {
                storeDirect.add(o);
            } else {
                storeDirect.add(o);

            }
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                callLifecycleMethod(PreStore.class, storeInBg);
                config.getWriter().store(storeInBg);
                callLifecycleMethod(PostStore.class, storeInBg);
            }
        });
        callLifecycleMethod(PreStore.class, storeDirect);
        config.getWriter().store(storeDirect);
        callLifecycleMethod(PostStore.class, storeDirect);

    }

    public void delete(Query o) {
        if (!isAnnotationPresentInHierarchy(o.getClass(), NoProtection.class)) {
            if (accessDenied(o, Permission.DELETE)) {
                throw new SecurityException("Deletion of Object denied!");
            }
        }
        callLifecycleMethod(PreRemove.class, o);
        firePreRemoveEvent(o);

        Cache cc = getAnnotationFromHierarchy(o.getType(), Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || isAnnotationPresentInHierarchy(o.getType(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().delete(o);
            callLifecycleMethod(PostRemove.class, o);
            firePostRemoveEvent(o);
            return;
        }
        final Query fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().delete(fo);
                firePostRemoveEvent(fo);

            }
        });
        inc(StatisticKeys.WRITES_CACHED);
        firePostRemoveEvent(o);
    }

    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o
     */
    public void delete(Object o) {
        if (o instanceof Query) {
            delete((Query) o);
            return;
        }
        o = getRealObject(o);
        if (!isAnnotationPresentInHierarchy(o.getClass(), NoProtection.class)) {
            if (accessDenied(o, Permission.DELETE)) {
                throw new SecurityException("Deletion of Object denied!");
            }
        }
        firePreRemoveEvent(o);

        Cache cc = getAnnotationFromHierarchy(o.getClass(), Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().delete(o);
            firePostRemoveEvent(o);
            return;
        }
        final Object fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().delete(fo);
                firePostRemoveEvent(fo);
            }
        });
        inc(StatisticKeys.WRITES_CACHED);
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


    public boolean isWriteCached(Class<?> cls) {
        Cache c = getAnnotationFromHierarchy(cls, Cache.class);
        if (isAnnotationPresentInHierarchy(cls, NoCache.class) || c == null || !c.writeCache()) {
            return false;
        }
        return true;

    }

    public <T, R> Aggregator<T, R> createAggregator(Class<T> type, Class<R> resultType) {
        Aggregator<T, R> aggregator = config.getAggregatorFactory().createAggregator(type, resultType);
        aggregator.setMorphium(this);
        return aggregator;
    }

    public <T, R> List<R> aggregate(Aggregator<T, R> a) {
        DBCollection coll = database.getCollection(config.getMapper().getCollectionName(a.getSearchType()));
        List<DBObject> agList = a.toAggregationList();
        DBObject first = agList.get(0);
        agList.remove(0);
        AggregationOutput resp = coll.aggregate(first, agList.toArray(new DBObject[agList.size()]));

        List<R> ret = new ArrayList<R>();
        for (DBObject o : resp.results()) {
            ret.add(getMapper().unmarshall(a.getResultType(), o));
        }
        return ret;
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

    public <T> MongoField<T> createMongoField() {
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
        if (isAnnotationPresentInHierarchy(cls, NoProtection.class)) {
            return false;
        }
        if (privileged.contains(Thread.currentThread())) {
            privileged.remove(Thread.currentThread());
            return false;
        }
        return !getSecurityManager().checkAccess(cls, p);
    }

    public boolean accessDenied(Object r, Permission p) throws MongoSecurityException {
        if (isAnnotationPresentInHierarchy(r.getClass(), NoProtection.class)) {
            return false;
        }
        if (privileged.contains(Thread.currentThread())) {
            privileged.remove(Thread.currentThread());
            return false;
        }

        return !getSecurityManager().checkAccess(config.getMapper().getRealObject(r), p);
    }


}
