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
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.cache.CacheHousekeeper;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.ConfNode;
import de.caluga.morphium.replicaset.ReplicaSetConf;
import de.caluga.morphium.replicaset.ReplicaSetNode;
import de.caluga.morphium.validation.JavaxValidationStorageListener;
import de.caluga.morphium.writer.WriterImpl;
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

@SuppressWarnings("UnusedDeclaration")
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

    private final Map<StatisticKeys, StatisticValue> stats;
    private Map<Class<?>, Map<Class<? extends Annotation>, Method>> lifeCycleMethods;
    /**
     * String Representing current user - needs to be set by Application
     */
//    private String currentUser;
    private CacheHousekeeper cacheHousekeeper;

    private List<MorphiumStorageListener> listeners;
    private Vector<ProfilingListener> profilingListeners;
    private Vector<ShutdownListener> shutDownListeners;

    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();
    private MorphiumCache cache;
    private ObjectMapper objectMapper;

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
        shutDownListeners = new Vector<ShutdownListener>();
        listeners = new ArrayList<MorphiumStorageListener>();
        profilingListeners = new Vector<ProfilingListener>();


        stats = new Hashtable<StatisticKeys, StatisticValue>();
        lifeCycleMethods = new Hashtable<Class<?>, Map<Class<? extends Annotation>, Method>>();
        for (StatisticKeys k : StatisticKeys.values()) {
            stats.put(k, new StatisticValue());
        }

        MongoOptions o = new MongoOptions();
        o.setAutoConnectRetry(config.isAutoreconnect());
        o.setSafe(config.isSafeMode());
        o.setFsync(config.isGlobalFsync());
        o.setSocketTimeout(config.getSocketTimeout());
        o.setConnectTimeout(config.getConnectionTimeout());
        o.setConnectionsPerHost(config.getMaxConnections());
        o.setSocketKeepAlive(config.isSocketKeepAlive());
        o.setThreadsAllowedToBlockForConnectionMultiplier(config.getBlockingThreadsMultiplier());
        o.setJ(config.isGlobalJ());
        o.setW(config.getGlobalW());
        o.setWtimeout(config.getWriteTimeout());
        o.setMaxAutoConnectRetryTime(config.getMaxAutoReconnectTime());
        o.setMaxWaitTime(config.getMaxWaitTime());


        writers.setCorePoolSize(config.getMaxConnections() / 2);
        writers.setMaximumPoolSize(config.getMaxConnections() + 1);

        if (config.getAdr().isEmpty()) {
            throw new RuntimeException("Error - no server address specified!");
        }
        mongo = new Mongo(config.getAdr(), o);
        database = mongo.getDB(config.getDatabase());
        if (config.getDefaultReadPreference() != null) {
            mongo.setReadPreference(config.getDefaultReadPreference().getPref());
        }
        if (config.getMongoLogin() != null) {
            if (!database.authenticate(config.getMongoLogin(), config.getMongoPassword().toCharArray())) {
                throw new RuntimeException("Authentication failed!");
            }
        }

        if (config.getConfigManager() == null) {
            config.setConfigManager(new ConfigManagerImpl());
        }
        config.getConfigManager().setMorphium(this);
        cacheHousekeeper = new CacheHousekeeper(this, 5000, config.getGlobalCacheValidTime());
        cacheHousekeeper.start();
        config.getConfigManager().startCleanupThread();
        if (config.getWriter() == null) {
            config.setWriter(new WriterImpl());
        }
        config.getWriter().setMorphium(this);

        cache = config.getCache();

        // enable/disable javax.validation support
        if (hasValidationSupport()) {
            logger.info("Adding javax.validation Support...");
            addListener(new JavaxValidationStorageListener());
        }
        try {
            objectMapper = config.getOmClass().newInstance();
            objectMapper.setMorphium(this);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        logger.info("Initialization successful...");

    }

    public MorphiumCache getCache() {
        return config.getCache();
    }

    /**
     * Checks if javax.validation is available and enables validation support.
     *
     * @return true, if validation is supported
     */

    @SuppressWarnings("UnusedDeclaration")
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
     * @param template - what to search for
     * @param fields   - fields to use for searching
     * @param <T>      - type
     * @return result of search
     */
    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public <T> List<T> findByTemplate(T template, String... fields) {
        Class cls = template.getClass();
        List<String> flds = new ArrayList<String>();
        if (fields.length > 0) {
            flds.addAll(Arrays.asList(fields));
        } else {
            flds = annotationHelper.getFields(cls);
        }
        Query<T> q = createQueryFor((Class<T>) cls);
        for (String f : flds) {
            try {
                q.f(f).eq(annotationHelper.getValue(template, f));
            } catch (Exception e) {
                logger.error("Could not read field " + f + " of object " + cls.getName());
            }
        }
        return q.asList();
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void unset(Object toSet, Enum field) {
        unset(toSet, field.name());
    }

    public void unset(final Object toSet, final String field) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");

        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
        Cache c = annotationHelper.getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().unset(toSet, field, null);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().unset(toSet, field, null);
                firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.UNSET);
            }
        });
    }

    /**
     * can be called for autmatic index ensurance. Attention: might cause heavy load on mongo
     * will be called automatically if a new collection is created
     *
     * @param type type to ensure indices for
     */
    @SuppressWarnings("unchecked")
    public void ensureIndicesFor(Class type) {
        if (annotationHelper.isAnnotationPresentInHierarchy(type, Index.class)) {
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

            List<String> flds = annotationHelper.getFields(type, Index.class);
            if (flds != null && flds.size() > 0) {
                for (String f : flds) {
                    Index i = annotationHelper.getField(type, f).getAnnotation(Index.class);
                    if (i.decrement()) {
                        ensureIndex(type, "-" + f);
                    } else {
                        ensureIndex(type, f);
                    }
                }
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

    @SuppressWarnings({"unchecked", "UnusedDeclaration", "SuspiciousMethodCalls"})
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

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
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

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pull(Query<?> query, Enum field, Object value, boolean insertIfNotExist, boolean multiple) {
        pull(query, field.name(), value, insertIfNotExist, multiple);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pushAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        push(query, field.name(), value, insertIfNotExist, multiple);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void pullAll(Query<?> query, Enum field, List<Object> value, boolean insertIfNotExist, boolean multiple) {
        pull(query, field.name(), value, insertIfNotExist, multiple);
    }


    public void push(final Query<?> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
        if (!annotationHelper.isWriteCached(query.getType())) {
            config.getWriter().pushPull(true, query, field, value, insertIfNotExist, multiple, null);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPull(true, query, field, value, insertIfNotExist, multiple, null);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            }
        });
    }

    public void pull(final Query<?> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
        if (!annotationHelper.isWriteCached(query.getType())) {
            config.getWriter().pushPull(false, query, field, value, insertIfNotExist, multiple, null);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPull(false, query, field, value, insertIfNotExist, multiple, null);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PULL);
            }
        });
    }

    public void pushAll(final Query<?> query, final String field, final List<?> value, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null || field == null) throw new RuntimeException("Cannot update null!");

        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
        if (!annotationHelper.isWriteCached(query.getType())) {
            config.getWriter().pushPullAll(true, query, field, value, insertIfNotExist, multiple, null);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().pushPullAll(true, query, field, value, insertIfNotExist, multiple, null);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
            }
        });

    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
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

        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = annotationHelper.getAnnotationFromHierarchy(query.getType(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(query.getType(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().set(query, map, insertIfNotExist, multiple, null);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(query, map, insertIfNotExist, multiple, null);
                firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
            }
        });
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void dec(Query<?> query, Enum field, int amount, boolean insertIfNotExist, boolean multiple) {
        dec(query, field.name(), amount, insertIfNotExist, multiple);
    }

    public void dec(Query<?> query, String field, int amount, boolean insertIfNotExist, boolean multiple) {
        inc(query, field, -amount, insertIfNotExist, multiple);
    }

    public void dec(Query<?> query, String field, int amount) {
        inc(query, field, -amount, false, false);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void dec(Query<?> query, Enum field, int amount) {
        inc(query, field, -amount, false, false);
    }

    public void inc(Query<?> query, String field, int amount) {
        inc(query, field, amount, false, false);
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public void inc(Query<?> query, Enum field, int amount) {
        inc(query, field, amount, false, false);
    }

    public void inc(Query<?> query, Enum field, int amount, boolean insertIfNotExist, boolean multiple) {
        inc(query, field.name(), amount, insertIfNotExist, multiple);
    }

    public void inc(final Query<?> query, final String name, final int amount, final boolean insertIfNotExist, final boolean multiple) {
        if (query == null) throw new RuntimeException("Cannot update null!");

        firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
        Cache c = annotationHelper.getAnnotationFromHierarchy(query.getType(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(query.getType(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().inc(query, name, amount, insertIfNotExist, multiple, null);
            firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().inc(query, name, amount, insertIfNotExist, multiple, null);
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

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = annotationHelper.getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().set(toSet, field, value, null);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(toSet, field, value, null);
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

        if (getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet);
            return;
        }
        firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
        Cache c = annotationHelper.getAnnotationFromHierarchy(toSet.getClass(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(toSet.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().inc(toSet, field, i, null);
            firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().set(toSet, field, i, null);
                firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            }
        });
    }


    public void inc(StatisticKeys k) {
        stats.get(k).inc();
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public String toJsonString(Object o) {
        return objectMapper.marshall(o).toString();
    }


    public int writeBufferCount() {
        return writers.getQueue().size();
    }


    /**
     * updating an enty in DB without sending the whole entity
     * only transfers the fields to be changed / set
     *
     * @param ent    - entity to update
     * @param fields - fields to use
     */
    public void updateUsingFields(final Object ent, final String... fields) {
        if (ent == null) return;
        if (fields.length == 0) return; //not doing an update - no change

        if (annotationHelper.isAnnotationPresentInHierarchy(ent.getClass(), NoCache.class)) {
            config.getWriter().storeUsingFields(ent, null, fields);
            return;
        }

        firePreUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
        Cache c = annotationHelper.getAnnotationFromHierarchy(ent.getClass(), Cache.class);
        if (annotationHelper.isAnnotationPresentInHierarchy(ent.getClass(), NoCache.class) || c == null || !c.writeCache()) {
            config.getWriter().storeUsingFields(ent, null, fields);
            firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            return;
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().storeUsingFields(ent, null, fields);
                firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            }
        });
    }

    public List<Annotation> getAllAnnotationsFromHierachy(Class<?> cls, Class<? extends Annotation>... anCls) {
        cls = annotationHelper.getRealClass(cls);
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


    public ObjectMapper getMapper() {
        return objectMapper;
    }

    public AnnotationAndReflectionHelper getARHelper() {
        return annotationHelper;
    }


    public void callLifecycleMethod(Class<? extends Annotation> type, Object on) {
        if (on == null) return;
        //No synchronized block - might cause the methods to be put twice into the
        //hashtabel - but for performance reasons, it's ok...
        Class<?> cls = on.getClass();
        //No Lifecycle annotation - no method calling
        if (!annotationHelper.isAnnotationPresentInHierarchy(cls, Lifecycle.class)) {//cls.isAnnotationPresent(Lifecycle.class)) {
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
     * @param o   - object to read
     * @param <T> - tpye of the object
     * @return -  entity
     */
    public <T> T reread(T o) {
        if (o == null) throw new RuntimeException("Cannot re read null!");
        ObjectId id = getId(o);
        if (id == null) {
            return null;
        }
        DBCollection col = database.getCollection(objectMapper.getCollectionName(o.getClass()));
        BasicDBObject srch = new BasicDBObject("_id", id);
        DBCursor crs = col.find(srch).limit(1);
        if (crs.hasNext()) {
            DBObject dbo = crs.next();
            Object fromDb = objectMapper.unmarshall(o.getClass(), dbo);
            List<String> flds = annotationHelper.getFields(o.getClass());
            for (String f : flds) {
                Field fld = annotationHelper.getField(o.getClass(), f);
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

    @SuppressWarnings("unchecked")
    public void firePreStoreEvent(Object o, boolean isNew) {
        if (o == null) return;
        for (MorphiumStorageListener l : listeners) {
            l.preStore(this, o, isNew);
        }
        callLifecycleMethod(PreStore.class, o);

    }

    @SuppressWarnings("unchecked")
    public void firePostStoreEvent(Object o, boolean isNew) {
        for (MorphiumStorageListener l : listeners) {
            l.postStore(this, o, isNew);
        }
        callLifecycleMethod(PostStore.class, o);
        //existing object  => store last Access, if needed

    }

    @SuppressWarnings("unchecked")
    public void firePreDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.preDrop(this, cls);
        }

    }


    @SuppressWarnings("unchecked")
    public void firePostDropEvent(Class cls) {
        for (MorphiumStorageListener l : listeners) {
            l.postDrop(this, cls);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePostUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.postUpdate(this, cls, t);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePreUpdateEvent(Class cls, MorphiumStorageListener.UpdateTypes t) {
        for (MorphiumStorageListener l : listeners) {
            l.preUpdate(this, cls, t);
        }
    }

    @SuppressWarnings("unchecked")
    public void firePostRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(this, o);
        }
        callLifecycleMethod(PostRemove.class, o);
    }

    @SuppressWarnings("unchecked")
    public void firePostRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.postRemove(this, q);
        }
        //TODO: FIX - Cannot call lifecycle method here
    }

    @SuppressWarnings("unchecked")
    public void firePreRemoveEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.preDelete(this, o);
        }
        callLifecycleMethod(PreRemove.class, o);
    }

    @SuppressWarnings("unchecked")
    public void firePreRemoveEvent(Query q) {
        for (MorphiumStorageListener l : listeners) {
            l.preRemove(this, q);
        }
        //TODO: Fix - cannot call lifecycle method
    }

    /**
     * will be called by query after unmarshalling
     *
     * @param o - entitiy
     */
    @SuppressWarnings("unchecked")
    public void firePostLoadEvent(Object o) {
        for (MorphiumStorageListener l : listeners) {
            l.postLoad(this, o);
        }
        callLifecycleMethod(PostLoad.class, o);
    }


    /**
     * same as retReplicaSetStatus(false);
     *
     * @return replica set status
     */
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus() {
        return getReplicaSetStatus(false);
    }

    /**
     * get the current replicaset status - issues the replSetGetStatus command to mongo
     * if full==true, also the configuration is read. This method is called with full==false for every write in
     * case a Replicaset is configured to find out the current number of active nodes
     *
     * @param full - if true- return full status
     * @return status
     */
    @SuppressWarnings("unchecked")
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus(boolean full) {
        if (config.getAdr().size() > 1) {
            try {
                CommandResult res = getMongo().getDB("admin").command("replSetGetStatus");
                de.caluga.morphium.replicaset.ReplicaSetStatus status = objectMapper.unmarshall(de.caluga.morphium.replicaset.ReplicaSetStatus.class, res);
                if (full) {
                    DBCursor rpl = getMongo().getDB("local").getCollection("system.replset").find();
                    DBObject stat = rpl.next(); //should only be one, i think
                    ReplicaSetConf cfg = objectMapper.unmarshall(ReplicaSetConf.class, stat);
                    List<Object> mem = cfg.getMemberList();
                    List<ConfNode> cmembers = new ArrayList<ConfNode>();

                    for (Object o : mem) {
                        DBObject dbo = (DBObject) o;
                        ConfNode cn = objectMapper.unmarshall(ConfNode.class, dbo);
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
                    ReplicaSetNode n = objectMapper.unmarshall(ReplicaSetNode.class, o);
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
        return config.getAdr().size() > 1;
    }

    @SuppressWarnings("ConstantConditions")
    public WriteConcern getWriteConcernForClass(Class<?> cls) {
        if (logger.isDebugEnabled()) logger.debug("returning write concern for " + cls.getSimpleName());
        WriteSafety safety = annotationHelper.getAnnotationFromHierarchy(cls, WriteSafety.class);  // cls.getAnnotation(WriteSafety.class);
        if (safety == null) return null;
        @SuppressWarnings("deprecation") boolean fsync = safety.waitForSync();
        boolean j = safety.waitForJournalCommit();

        if (j && fsync) {
            fsync = false;
        }
        int w = safety.level().getValue();
        if (!isReplicaSet() && w > 1) {
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

            int masterOpTime = 0;
            int maxReplLag = 0;
            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 1) {
                    //Master
                    masterOpTime = node.getOptime().getTime();
                }
            }
            for (ReplicaSetNode node : s.getMembers()) {
                if (node.getState() == 2) {
                    //Master
                    int tm = node.getOptime().getTime() - masterOpTime;
                    if (maxReplLag < tm) {
                        maxReplLag = tm;
                    }
                }
            }
            if (timeout < 0) {
                //set timeout to replication lag * 3 - just to be sure
                if (logger.isDebugEnabled()) {
                    logger.debug("Setting timeout to replication lag*3");
                }
                if (maxReplLag < 0) {
                    maxReplLag = -maxReplLag;
                }
                if (maxReplLag == 0) maxReplLag = 1;
                timeout = maxReplLag * 3000;
                if (maxReplLag > 10) {
                    logger.warn("Warning: replication lag too high! timeout set to " + timeout + "ms - replication Lag is " + maxReplLag + "s - write should take place in Background!");
                }

//                if (getConfig().getConnectionTimeout() == 0) {
//                    if (logger.isDebugEnabled())
//                        logger.debug("Not waiting for all slaves withoug timeout - unfortunately no connection timeout set in config - setting to 10s, Type: " + cls.getSimpleName());
//                    timeout = 10000;
//                } else {
//                    if (logger.isDebugEnabled())
//                        logger.debug("Not waiting for all slaves without timeout - could cause deadlock. Setting to connectionTimeout value, Type: " + cls.getSimpleName());
//                    timeout = getConfig().getConnectionTimeout();
//                }
            }
            //Wait for all active slaves
            w = activeNodes;
            if (timeout > 0 && timeout < maxReplLag * 1000) {
                logger.warn("Timeout is set smaller than replication lag - increasing to replication_lag time * 3");
                timeout = maxReplLag * 3000;
            }
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


    /**
     * issues a delete command - no lifecycle methods calles, no drop, keeps all indexec this way
     *
     * @param cls - class
     */
    @SuppressWarnings("unchecked")
    public void clearCollection(Class<?> cls) {
        firePreDropEvent(cls);
        delete(createQueryFor(cls));
        firePostDropEvent(cls);
    }

    /**
     * clears every single object in collection - reads ALL objects to do so
     * this way Lifecycle methods can be called!
     *
     * @param cls -class
     */

    public void clearCollectionOneByOne(Class<?> cls) {
        inc(StatisticKeys.WRITES);
        List<?> lst = readAll(cls);
        for (Object r : lst) {
            delete(r);
        }

        cache.clearCacheIfNecessary(cls);


    }

    /**
     * return a list of all elements stored in morphium for this type
     *
     * @param cls - type to search for, needs to be an Property
     * @param <T> - Type
     * @return - list of all elements stored
     */
    public <T> List<T> readAll(Class<? extends T> cls) {
        inc(StatisticKeys.READS);
        Query<T> qu;
        qu = createQueryFor(cls);
        return qu.asList();
    }

    public <T> Query<T> createQueryFor(Class<? extends T> type) {
        Query<T> q = config.getQueryFact().createQuery(this, type);
        q.setMorphium(this);
        return q;
    }

    public <T> List<T> find(Query<T> q) {
        return q.asList();
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
    @SuppressWarnings("unchecked")
    public List<Object> distinct(String key, Query q) {
        return database.getCollection(objectMapper.getCollectionName(q.getType())).distinct(key, q.toQueryObject());
    }

    @SuppressWarnings("unchecked")
    public List<Object> distinct(String key, Class cls) {
        DBCollection collection = database.getCollection(objectMapper.getCollectionName(cls));
        setReadPreference(collection, cls);
        return collection.distinct(key, new BasicDBObject());
    }

    private void setReadPreference(DBCollection c, Class type) {
        DefaultReadPreference pr = annotationHelper.getAnnotationFromHierarchy(type, DefaultReadPreference.class);
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
        GroupCommand cmd = new GroupCommand(database.getCollection(objectMapper.getCollectionName(q.getType())),
                k, q.toQueryObject(), ini, jsReduce, jsFinalize);
        return database.getCollection(objectMapper.getCollectionName(q.getType())).group(cmd);
    }

    @SuppressWarnings("unchecked")
    public <T> T findById(Class<? extends T> type, ObjectId id) {
        T ret = cache.getFromIDCache(type, id);
        if (ret != null) return ret;
        List<String> ls = annotationHelper.getFields(type, Id.class);
        if (ls.size() == 0) throw new RuntimeException("Cannot find by ID on non-Entity");

        return createQueryFor(type).f(ls.get(0)).eq(id).get();
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
    public <T> List<T> findByField(Class<? extends T> cls, String fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
//        return createQueryFor(cls).field(fld).equal(val).asList();
    }

    public <T> List<T> findByField(Class<? extends T> cls, Enum fld, Object val) {
        Query<T> q = createQueryFor(cls);
        q = q.f(fld).eq(val);
        return q.asList();
//        return createQueryFor(cls).field(fld).equal(val).asList();
    }


    /**
     * Erase cache entries for the given type. is being called after every store
     * depending on cache settings!
     *
     * @param cls - class
     */
    public void clearCachefor(Class<?> cls) {
        cache.clearCachefor(cls);
    }

    public void storeNoCache(Object lst) {
        config.getWriter().store(lst, null);
    }

    public void storeInBackground(final Object lst) {
        inc(StatisticKeys.WRITES_CACHED);
        writers.execute(new Runnable() {
            @Override
            public void run() {
                boolean isNew = getId(lst) == null;
                firePreStoreEvent(lst, isNew);
                config.getWriter().store(lst, null);
                firePostStoreEvent(lst, isNew);
            }
        });
    }


    public ObjectId getId(Object o) {
        return annotationHelper.getId(o);
    }

    public void dropCollection(Class<?> cls) {
        if (annotationHelper.isAnnotationPresentInHierarchy(cls, Entity.class)) {
            firePreDropEvent(cls);
            long start = System.currentTimeMillis();
//            Entity entity = annotationHelper.getAnnotationFromHierarchy(cls, Entity.class); //cls.getAnnotation(Entity.class);

            DBCollection coll = database.getCollection(objectMapper.getCollectionName(cls));
//            coll.setReadPreference(com.mongodb.ReadPreference.PRIMARY);
            coll.drop();
            long dur = System.currentTimeMillis() - start;
            fireProfilingWriteEvent(cls, null, dur, false, WriteAccessType.DROP);
            firePostDropEvent(cls);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    public void ensureIndex(Class<?> cls, Map<String, Object> index) {
        List<String> fields = annotationHelper.getFields(cls);

        Map<String, Object> idx = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> es : index.entrySet()) {
            String k = es.getKey();
            if (!fields.contains(k) && !fields.contains(annotationHelper.convertCamelCase(k))) {
                throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
            }
            String fn = annotationHelper.getFieldName(cls, k);
            idx.put(fn, es.getValue());
        }
        long start = System.currentTimeMillis();
        BasicDBObject keys = new BasicDBObject(idx);
        database.getCollection(objectMapper.getCollectionName(cls)).ensureIndex(keys);
        long dur = System.currentTimeMillis() - start;
        fireProfilingWriteEvent(cls, keys, dur, false, WriteAccessType.ENSURE_INDEX);
    }

    /**
     * ensureIndex(CachedObject.class,"counter","-value");
     * ensureIndex(CachedObject.class,"counter:2d","-value);
     * Similar to sorting
     *
     * @param cls    - class
     * @param fldStr - fields
     */
    public void ensureIndex(Class<?> cls, String... fldStr) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (String f : fldStr) {
            int idx = 1;
            if (f.contains(":")) {
                //explicitly defined index
                String fs[] = f.split(":");
                m.put(fs[0], fs[1]);
            } else {
                if (f.startsWith("-")) {
                    idx = -1;
                    f = f.substring(1);
                } else if (f.startsWith("+")) {
                    f = f.substring(1);
                }
                m.put(f, idx);
            }
        }
        ensureIndex(cls, m);
    }

    public void ensureIndex(Class<?> cls, Enum... fldStr) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
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

        Class<?> type = annotationHelper.getRealClass(o.getClass());
        final boolean isNew = getId(o) == null;
        firePreStoreEvent(o, isNew);
        Cache cc = annotationHelper.getAnnotationFromHierarchy(type, Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || annotationHelper.isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().store(o, null);
            firePostStoreEvent(o, isNew);
            return;
        }
        final Object fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().store(fo, null);
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
            Cache c = annotationHelper.getAnnotationFromHierarchy(o.getClass(), Cache.class);//o.getClass().getAnnotation(Cache.class);
            if (annotationHelper.isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || c == null || !c.writeCache()) {
                storeDirect.add(o);
            } else {
                storeDirect.add(o);

            }
        }
        writers.execute(new Runnable() {
            @Override
            public void run() {
                callLifecycleMethod(PreStore.class, storeInBg);
                config.getWriter().store(storeInBg, (AsyncOperationCallback<List<T>>) null);
                callLifecycleMethod(PostStore.class, storeInBg);
            }
        });
        callLifecycleMethod(PreStore.class, storeDirect);
        config.getWriter().store(storeDirect, (AsyncOperationCallback<List<T>>) null);
        callLifecycleMethod(PostStore.class, storeDirect);

    }

    public <T> void delete(Query<T> o) {
        callLifecycleMethod(PreRemove.class, o);
        firePreRemoveEvent(o);

        Cache cc = annotationHelper.getAnnotationFromHierarchy(o.getType(), Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || annotationHelper.isAnnotationPresentInHierarchy(o.getType(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().delete(o, (AsyncOperationCallback<T>) null);
            callLifecycleMethod(PostRemove.class, o);
            firePostRemoveEvent(o);
            return;
        }
        final Query<T> fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().delete(fo, (AsyncOperationCallback<T>) null);
                firePostRemoveEvent(fo);

            }
        });
        inc(StatisticKeys.WRITES_CACHED);
        firePostRemoveEvent(o);
    }

    /**
     * deletes a single object from morphium backend. Clears cache
     *
     * @param o - entity
     */
    public void delete(Object o) {
        if (o instanceof Query) {
            delete((Query) o);
            return;
        }
        o = annotationHelper.getRealObject(o);
        firePreRemoveEvent(o);

        Cache cc = annotationHelper.getAnnotationFromHierarchy(o.getClass(), Cache.class);//o.getClass().getAnnotation(Cache.class);
        if (cc == null || annotationHelper.isAnnotationPresentInHierarchy(o.getClass(), NoCache.class) || !cc.writeCache()) {
            config.getWriter().delete(o, null);
            firePostRemoveEvent(o);
            return;
        }
        final Object fo = o;
        writers.execute(new Runnable() {
            @Override
            public void run() {
                config.getWriter().delete(fo, null);
                firePostRemoveEvent(fo);
            }
        });
        inc(StatisticKeys.WRITES_CACHED);
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
        return annotationHelper.createCamelCase(n, false);
    }


    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        Aggregator<T, R> aggregator = config.getAggregatorFactory().createAggregator(type, resultType);
        aggregator.setMorphium(this);
        return aggregator;
    }

    public <T, R> List<R> aggregate(Aggregator<T, R> a) {
        DBCollection coll = database.getCollection(objectMapper.getCollectionName(a.getSearchType()));
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
     * @param o   - entity
     * @param <T> - type
     * @return Type
     */
    @SuppressWarnings("unchecked")
    public <T> T createPartiallyUpdateableEntity(T o) {
        return (T) Enhancer.create(o.getClass(), new Class[]{PartiallyUpdateable.class, Serializable.class}, new PartiallyUpdateableProxy(this, o));
    }

    @SuppressWarnings("unchecked")
    public <T> T createLazyLoadedEntity(Class<? extends T> cls, ObjectId id) {
        return (T) Enhancer.create(cls, new Class[]{Serializable.class}, new LazyDeReferencingProxy(this, cls, id));
    }

    @SuppressWarnings("unchecked")
    public <T> MongoField<T> createMongoField() {
        try {
            return (MongoField<T>) config.getFieldImplClass().newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


}
